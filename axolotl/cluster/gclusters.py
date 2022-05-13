"""
# Global clustering based on TNF and abundance
# by zhong wang @lbl.gov

# testing 
from pyspark.sql.functions import *
from pyspark.sql.types import *

from sparc.samples import *
from sparc.gclusters import *
# java udf kmer function
spark.udf.registerJavaFunction("jkmerudf", "org.jgi.spark.udf.jkmer", ArrayType(LongType()))
# need to install umap
!pip install --quiet umap-learn

#parameters
DEBUG=True
k=31
m = 17
n = 0
rkmers = 100
min_reads_per_cluster=10 # clusters with fewer numbers will be ignored
output_prefix = 'dbfs:/mnt/share.jgi-ga.org/contamination/zymo/pbio-2296.21123.ccs.filter'
key_file = 'dbfs:/mnt/share.jgi-ga.org/contamination/zymo/zymo_keys.csv'

from pyspark.sql import SparkSession
 
spark = (SparkSession
         .builder
         .appName("sparc")
         .config("spark.network.timeout", "10h")
         .config("spark.executor.heartbeatInterval", "5h")
         .config("spark.storage.blockManagerSlaveTimeoutMs", "5h")
         .config("spark.driver.maxResultSize", "8192M")
         .config("spark.sql.files.maxPartitionBytes", "32M")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1") # need to turn off the autobroadcast join
         .getOrCreate())

samples, total_reads = load_samples(output_prefix + '_samples_meta.json', key='all')
gclusters = global_clustering(output_prefix,
                   samples, # samples object, with reads and kmers calculated, required
                   k=k, # k-mer length  
                   m=m,
                   key_file=key_file,
                   min_reads_per_cluster=min_reads_per_cluster, # clusters with fewer numbers will be ignored
                   rkmers=rkmers # Global clustering only: number of representative kmer/minimizers per cluster to sample 
)

"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Window

from sklearn import metrics
import umap
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN

from sparc.samples import *
from sparc.utils import *
from sparc.evaluation import *

def global_clustering(output_prefix,
                    samples='', # samples
                    lclusters=None,
                    k=31, # k-mer length for coverage estimation, use the same k,m for kmerMapping 
                    m=17,                   
                    min_reads_per_cluster=2, # local clusters with fewer numbers will be ignored
                    rkmers=10, # Global clustering only: number of representative kmer/minimizers per cluster to sample
                    key_file='', # Optional for clustering accuracy evaluation: answer key csv file, in the format: read_name\tref_id, ref_id needs to be numerical
                    DEBUG=False
                    ):
  
    """
    global clustering
    1) compute the similarity between clusters based on k-mers
    2) generate a square similarity matrix, and then do clustering
    """
    print("Golbal clustering parameters: ")
    print("Number of representative kmers to sample: %d" % rkmers)
    print("Minimum reads per cluster: %d" % min_reads_per_cluster)
    print("Ground truth file: %s" % key_file) if key_file else print("No ground truth file provided.")   
    print("Start Global clustering with multiple samples ... ")
    spark = SparkSession.getActiveSession()
     # merge reads from all samples
    if len(samples) == 1:
        reads_df = samples[0].reads
    else:    
        reads_df = reduce(DataFrame.unionByName, [s.reads for s in samples])
    # load local clustering dataframe: label(clusters), id(read)  
    if not lclusters:   
        lclusters = spark.read.parquet(output_prefix+'_localclusters')
    # filter clusters and obtain clustered reads
    clusters = reads_df.join(lclusters, on='id', how='left')
    clusters = (clusters
                .groupby('label')
                .agg(F.count(F.lit(1)).alias('lcluster_size'), F.collect_list('Seq').alias('Seqs'))
                .where(F.col('lcluster_size') >= min_reads_per_cluster)
    ).cache()

    # calculate tnf and coverage vectors
    ## estimate tnf vector
    tnf_udf = F.udf(lambda arr: reads_tnf(arr, method='median', sample=1000), ArrayType(FloatType()))
    ## estimate kmer coverage

    clusters.select('label', F.concat_ws('N', 'Seqs').alias('seq')).createOrReplaceTempView("reads")
    query = 'SELECT label, jkmerudf(seq, {}, {}) as kmerlist from reads'.format(k,m)
    kmers_df = spark.sql(query)
    kmers_df = (kmers_df
                .select('label', F.explode("kmerlist").alias('kmer'))
                .groupby('label', 'kmer')
                .agg(F.count(F.lit(1)).alias("kmer_count"))
 #               .where(F.col("kmer_count") >1)
            )
    # select a few rkmers to estimate cluster coverage        
    kmers_df = (kmers_df
    .groupby('label')
    .agg(F.collect_list('kmer').alias('kmers'))
    .select('label', F.explode(F.slice('kmers', 1, rkmers)).alias('kmer'))
    .repartition('label') 
    ).cache()

    count = 0
    rkmer_window = Window.partitionBy('label')
    median_func = F.expr('percentile_approx("kmer_count", 0.5)')
    for s in samples:
        if hasattr(s, 'cov'):
            pass
        else:
            setattr(s, 'cov')
        s.cov = (kmers_df
                    .join(s.kmers, on='kmer', how='left')
                    .groupby('label') # repartition to improve the performance windowing function
                    .agg(median_func.over(rkmer_window).alias('cov_'+str(count)))
        )
        if count == 0:
            cov = s.cov
        else:
            cov = cov.join(s.cov, on='label')  
        count +=1
    # cov is a dataframe, first column is label, then cov of each sample in a column
    col_to_concat = [F.col(c) for c in cov.columns]    
    cov = (cov
            .fillna(0)
            .select('label', F.array(*col_to_concat).alias('cov'))
    )
    tnf_udf = F.udf(lambda arr: reads_tnf(arr, method='median', sample=1000), ArrayType(FloatType())) 
    clusters = (clusters
                .join(cov, on='label')
                .withColumn('tnf', tnf_udf('Seqs'))
                .withColumn('tnf_cov', F.concat(F.col('cov'), F.col('tnf')))
                .drop(*('tnf', 'cov'))
    )

    # umap clustering
    tc_matrix = pd.DataFrame(clusters.select('tnf_cov').to_pandas_on_spark()['tnf_cov'].to_list()) # convert spark dataframe to pandas dataframe to use umap
    gclusters = umap_clustering(tnf=tc_matrix, clusters=clusters)

    print("Global clustering finished. " )

    print('Attempting to save final clustering results in fasta format...')
    # save final results
    try:
        (gclusters     
            .withColumn('seq', F.explode('seqs'))
            .withColumn('id',  F.monotonically_increasing_id())
            .withColumn('fa', F.concat_ws('', F.array(F.lit('>'), F.concat_ws('\n', 'id', 'seq')), F.lit('\n')))
            .select('glabel', 'fa')
            .groupby('glabel')
            .agg(F.concat_ws('', F.collect_list('fa')).alias('fa'))
            .repartition(gclusters.count())
            .select('fa')
            .write
            .option("quote", "\u0000")
            .mode('overwrite')
            .csv(output_prefix+'_gclusters', header=None)
        )   
    except:
        print("Failed to save the final results. It is possible that some of the clusters are bigger than 2GB?")
    # Estimate Accuracy
    if key_file:
        # print out estimated accuracy if reference/keys provided
        clusters = (gclusters
                        .select('label', 'glabel')
                        .join(lclusters, on='label')
                        .withColumn("merged_label", F.when(F.col('glabel') > -1, F.col('glabel')).otherwise(F.col('label')))
                        .drop('label', 'glabel')
                        .withColumnRenamed('merged_label', 'label')
        )
        read_ref_df = get_ref_key(key_file, reads_df)  
        get_cluster_accuracy(read_ref_df, clusters, showTop=20, min_reads_per_cluster=min_reads_per_cluster) 
    
    return gclusters

def umap_clustering(tnf='', clusters=''):
  """
  umap embedding and dbscan clustering
  """
  spark = SparkSession.getActiveSession()
  X = StandardScaler().fit_transform(tnf.iloc[:,1:])
  reducer = umap.UMAP(metric='cosine', random_state=1234)
  X = reducer.fit_transform(X)

  # select best eps parameters for dbscan
  min_samples = 2*(X.shape[1])
  if min_samples<2:
      min_samples=2
  best_eps = 0.0
  best_score = 0.0
  for eps in range(2,7,1): # scan for best eps
    eps = 0.1 * eps
    db = DBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1).fit(X)
    labels = db.labels_
    score = metrics.silhouette_score(X, labels)
    if score > best_score:
        best_eps = eps
        best_score = score
    
  db = DBSCAN(eps=best_eps, min_samples=min_samples, n_jobs=-1).fit(X)

  labels = db.labels_
  
  # Number of clusters in labels, ignoring noise if present.
  n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
  n_noise_ = list(labels).count(-1)
  print("Of %d total clusters, %d bigger clusters formed by DBSCAN, %d clusters remained" % (len(labels), n_clusters_, n_noise_))
  print("Silhouette Coefficient: %0.3f" % metrics.silhouette_score(X, labels))

  tnf['new_label'] = list(labels)
  tnf = tnf[[0, 'new_label']]
  # fix the data type 
  tnf = tnf.rename(columns={0:'label'})
  tnf['label'] = tnf['label'].astype(int)
  new_clusters = (clusters
                  .select('label', 'lcluster_size', 'seqs')
                  .join(spark.createDataFrame(tnf), on='label', how='left')
                  .withColumn("glabel", F.when(F.col('new_label') > -1, F.col('new_label')).otherwise(F.col('label')))
  )
  return new_clusters    