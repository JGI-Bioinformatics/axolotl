"""
Supplement pySpark.MLlib's clustering function, avoid GraphX's LPA inefficient implementation
Support for both weighted and unweighted LPA

Usage Example:

from axolotl.utils.vae import LPAcluster import *

vertex_df = load_data(input_path, input_type="TSV" )
edge_df = edge_gen(vertex_df, min_cs= .7)
lpa_cluster(vertex_df,
                edge_df,
                output, 
                min_edge_weight=0, # change it to a number between 0 and 1 to filter edge weights
                mode='wlpa', # "wlpa" or "lpa", default is wlpa, weighted LPA
                maxIter=15, # 15 in most cases is enough, smaller runs faster
                min_cluster_size=2, 
                checkpoint_dir='dbfs:/tmp/checkpoint' # tempary diretory to store intermediate clustering results, required
                )

Requirements:
  - The input data is either a TSV (with header), or a parquet format.
  - The first column is id, and the rest of columns are features

Limitations:
  - The supplied pairwise cosine similarity calculation is NOT scalable due to MLlib's implementation. When n is large, one node has to do n comparisions while others do less, with one node does only 1. This causes heavy data skewness.

TODO:
  - Support LibSVM format (standard MLlib format), numpy, pandas formats
"""

from axolotl.utils.spark import get_spark_session_and_context
import pyspark.sql.functions as F

# for cosine similarity calculation
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import IndexedRowMatrix, MatrixEntry, CoordinateMatrix, IndexedRow

# for clustering
from axolotl.utils.wlpa import *
import time


def load_data(input_path: str, input_type: str="parquet" ):
    """
    load input 
    """
    spark, sc = get_spark_session_and_context()
    # load input
    if input_type == "TSV":
        input_df = spark.read.csv(input_path, sep='\t', inferSchema=True, header=True)
    elif input_type == "parquet":
        input_df = spark.read(input_path)
    else:
        raise NotImplementedError("Unrecongnized input_type, can only be TSV or Parquet")
    return input_df

def edge_gen(input_df, min_cs: float = .7):
    """compute pairwise row cosine similarity"""
    
    # convert data to an indexed rowMatrix, transpose it, calculate column-cosine
    start_time = time.time()
    data = (input_df
            .rdd.map(lambda x: (x[0], Vectors.dense(x[1:])))
            )
    data = IndexedRowMatrix(data).toBlockMatrix()
    # data.validate();

    scores_exact = (data
        .transpose()
        .toIndexedRowMatrix()
        .columnSimilarities()
        .entries
        .filter(lambda entry: entry.value>0.7)
        .toDF(['src', 'dst', 'weight'])
    )
    print("Total number of edges passed filter %d" % scores_exact.count())
    time_elapsed = time.time() - start_time          
    print("Edge generation wall time %.3f seconds" % time_elapsed)  
    return scores_exact     

def lpa_cluster(vertex_df,
                edge_df,
                output, 
                min_edge_weight: float = 0,
                mode: str = 'wlpa', # "wlpa" or "lpa", default is wlpa, weighted LPA
                maxIter: int = 15, 
                min_cluster_size: int = 2,
                checkpoint_dir: str = 'dbfs:/tmp/checkpoint'
                ):
    """LPA Clustering"""    
    numPartitions = edge_df.rdd.getNumPartitions()
    if min_edge_weight > 0:
        edge_df = edge_df.repartition(5*numPartitions).where(F.col('weight')>= min_edge_weight)
    else:
        edge_df = edge_df.repartition(5*numPartitions)
    
    spark, sc = get_spark_session_and_context()
    sc.setCheckpointDir(checkpoint_dir)
    checkpoint_dir = spark.sparkContext.getCheckpointDir()

    start_time = time.time()
    if mode == 'lpa': # unweighted lpa
        edge_df = edge_df.drop('weight')
        clusters = run_wlpa(edge_df, maxIter=maxIter).cache()
    elif mode == 'wlpa': # weighted lpa
        clusters = run_wlpa(edge_df, maxIter=maxIter).cache()

    time_elapsed = time.time() - start_time
    orginal_id = vertex_df.columns[0]
    clusters = vertex_df.withColumnRenamed(orginal_id, 'id').join(clusters, on='id', how='inner')
    clusters = (clusters
        .groupby('label')
        .agg(F.count(F.lit(1)).alias('count'), F.collect_list('id').alias(orginal_id))
        .where(F.col('count') >= min_cluster_size)
    )
    # save result
    clusters.write.parquet(output)          

    print("Clustering Wall time %.3f seconds" % time_elapsed)  
    print("Calculating statistics ...")
    clustered_ = clusters.agg(F.sum('count')).collect()[0][0]
    # number of clusters
    num_clusters = clusters.count()
    if int(num_clusters) < 1:
        print("Clustering failed to produce any clusters. Please check your parameters and try again.")
    print('{:,d}'.format(num_clusters) + " clusters formed.")
    print('{:,d}'.format(clustered_) + " items are in clusters with at least " + str(min_cluster_size) + ".")  
    clusters.select('count').summary().show()

    # clean up checkpoints
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(checkpoint_dir)):
       fs.delete(sc._jvm.org.apache.hadoop.fs.Path(checkpoint_dir)) 