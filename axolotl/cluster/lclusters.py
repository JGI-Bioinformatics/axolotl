"""

# local clustering related functions
# by zhong wang @lbl.gov

# testing 

from sparc.lclusters import *
output_prefix = 'dbfs:/mnt/gis20mock/gis20mock/pacbio_test'
min_reads_per_cluster=2 # clusters with fewer numbers will be ignored

spark.sparkContext.setCheckpointDir('dbfs:/tmp/checkpoint')
checkpoint_dir = spark.sparkContext.getCheckpointDir()

lclusters = (local_clustering(output_prefix,
                     min_reads_per_cluster=min_reads_per_cluster, # clusters with fewer numbers will be ignored
                     DEBUG=True,
                     mode='wlpa',
                     numPartitions=numPartitions
                    )
            )
dbutils.fs.rm(checkpoint_dir,True)

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparc.wlpa import *
from sparc.utils import *
from sparc.evaluation import *


def local_clustering(output_prefix,
                    min_reads_per_cluster=2, # clusters with fewer numbers will be ignored
                    key_file='', # Optional for clustering accuracy evaluation: answer key csv file, in the format: read_name\tref_id, ref_id needs to be numerical
                    DEBUG=False,
                    numPartitions=200,
                    maxIter=5,
                    mode='wpla', # lpa=unweighted, wlpa=weigthed, otherwise, graphx's default LPA
                    min_edge_weight=2
                    ):
  
    print("Local clustering. ")
    print("Minimum reads per cluster: %d" % min_reads_per_cluster)
    print("Ground truth file: %s" % key_file) if key_file else print("No ground truth file provided.")
    spark = SparkSession.getActiveSession()
    vertex_df = spark.read.parquet(output_prefix +'_vertice').repartition(numPartitions, 'id')
    edge_df = spark.read.parquet(output_prefix +'_edges').repartition(numPartitions, 'src')
    edge_df = edge_df.where(F.col('weight')>= min_edge_weight)
    if DEBUG:
        print('{:,d}'.format(vertex_df.count()) + " vertice loaded.")
        print('{:,d}'.format(edge_df.count()) + " edges filtered.")

    # LPA: first round for raw reads, 10 iterations should be enough
    # clusters dataframe: label(Integer), count(Integer), Names(Array(String)), Seqs(Array(String))
    if mode == 'lpa': # unweighted lpa
        edge_df = edge_df.drop('weight')
        lclusters = run_wlpa(edge_df, maxIter=maxIter).cache()
    elif mode == 'wlpa': # weighted lpa
        lclusters = run_wlpa(edge_df, maxIter=maxIter).cache()
    else:
        from graphframes import GraphFrame
        lclusters = GraphFrame(vertex_df, edge_df).labelPropagation(maxIter=maxIter).cache()

     # save the local clustering results

    print("Saving local clustering results ...")
    save_parquet(lclusters, output_prefix, output_suffix='_localclusters', sort_col='label', overwrite=True)
    print("Local clustering finished.")

    if DEBUG:           
        print("Calculating statistics ...")
        clusters = vertex_df.join(lclusters, on='id', how='left')
        clusters = (clusters
        .groupby('label')
        .agg(F.count(F.lit(1)).alias('count'))
        .where(F.col('count') >= min_reads_per_cluster)).cache()
        
        clustered_reads = clusters.agg(F.sum('count')).collect()[0][0]
        # number of clusters
        num_local_clusters = clusters.count()
        if int(num_local_clusters) < 1:
            print("Clustering failed to produce any clusters. Please check your parameters and try again.")
            return None
        print('{:,d}'.format(num_local_clusters) + " clusters formed.")
        print('{:,d}'.format(clustered_reads) + " reads are in clusters with at least " + str(min_reads_per_cluster) + " reads.")     

    if key_file:
        # print out estimated accuracy if reference/keys provided
        print("Estimating clustering accuracy ...")
        read_ref_df = get_ref_key(spark, key_file, vertex_df)  
        get_cluster_accuracy(read_ref_df, lclusters, showTop=20, min_reads_per_cluster=min_reads_per_cluster)  

    return lclusters # return this for use in global or hybrid clustering  

