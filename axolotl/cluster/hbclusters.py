"""
This function is not in a working state.
# hybrid clustering related functions
# by zhong wang @lbl.gov

# testing 

from sparc.samples import *
from sparc.hbclusters import *

k=31 # k-mer length
k = 31
minimizer = True
m = int(k/2) + 1 if minimizer else k
n = 0
min_kmer_count = 2
min_edge_weight=2 # minimum number of shared kmers/minimizers between a pairs of reads to draw an edge
min_reads_per_cluster=2 # clusters with fewer numbers will be ignored
rkmers=10 # Global clustering only: number of representative kmer/minimizers per cluster to sample
min_similarity=0.90  #Global clustering only: cosine similarity threshhold to draw an edge between clusters 

# load samples and local clusters
samples, total_reads = load_samples(output_prefix + '_samples_meta.json', verbose=False)
lclusters = spark.read.parquet(output_prefix + '_localclusters')

hbclusters = (hybrid_clustering(output_prefix,
                     samples,
                     lclusters,
                     hybrid_samples,
                     k=31,
                     minimizer=False, # whether or not use minimizers to speed up computing
                     n=0, # number of minimizers/kmers per read returned, 0: all. Suggested read_length/k
                     min_kmer_count=2, # kmer frequency lower than this will be ignored (errors)
                     max_kmer_count=200, # kmer frequency over this will be igored, set it to a lower value for more heuristics                     
                     min_edge_weight=min_edge_weight, # minimum number of shared kmers/minimizers between a pairs of reads to draw an edge
                     min_reads_per_cluster=min_reads_per_cluster, # clusters with fewer numbers will be ignored
                     DEBUG = True
                    )
            )


"""

from pyspark.sql import DataFrame
from pyspark import StorageLevel
import pyspark.sql.functions as F
from pyspark.sql.types import *

from sparc.krmapper import *
from sparc.reads import *
from sparc.utils import *
from sparc.evaluation import *
from sparc.samples import *


def hybrid_clustering(output_prefix,
                     samples, # old samples (long reads)
                     lclusters, # previous local cluster (long reads)
                     hybrid_samples, # new samples (short reads)
                     min_reads_per_cluster=2, # clusters with fewer numbers will be ignored
                     key_file='', # Optional for clustering accuracy evaluation: answer key csv file, in the format: read_name\tref_id, ref_id needs to be numerical
                     DEBUG = False,
                     spark=''
                    ):
  
    print("Hyrbid clustering parameters: ")
    print("Minimum reads per cluster: %d" % min_reads_per_cluster)
    print("Ground truth file: %s" % key_file) if key_file else print("No ground truth file provided.")
    
    """
    hybrid clustering
    """
    
    print("Start hybrid clustering ... ")
    lclusters = lclusters.cache()
    # replace read_id with cluster label, s['kmers'] is dataframe ('kmer', 'kmer_count', 'Ids')
    for s in samples:
        s['kmers'] = (s['kmers']
                            .select('kmer', F.explode('Ids').alias('id')) # revert to kmer->id mapping
                            .join(lclusters, on='id', how='inner') 
                    )    #kmer -> read_id -> cluster_label

    # merge all samples           
    kmer_df = (reduce(DataFrame.unionByName, [s['kmers'] for s in samples])
            .groupby('kmer')
            .agg(F.collect_list('id').alias('Ids'), F.collect_set('label').alias('labels'))
    ) #kmer -> read_ids -> cluster_labels in orignial samples

    hkmer_df = (reduce(DataFrame.unionByName, [s['kmers'] for s in hybrid_samples])
            .groupby('kmer')
            .agg(F.flatten(F.collect_list('Ids')).alias('hIds'))
    ) #kmer -> read_ids in hybrid samples

    # now recruit kmer and reads from the hybrid samples
    mc_udf = F.udf(lambda x: Counter(x).most_common(1)[0], LongType())
    hbclusters = (hkmer_df
        .join(kmer_df, on='kmer', how='inner')
        .select(F.explode('hIds').alias('id'), 'labels')
        .groupby('id')
        .agg(F.flattern(F.collect_set('labels')).alias('labels'))
        .select('id', mc_udf('labels').alias('label')) # read_id -> cluster_label in hybrid samples
        .union(lclusters)
    ) 

    # save the local clustering results
    print("Saving hybrid clustering results ...")
    save_parquet(hbclusters, output_prefix, output_suffix='_hybridclusters', sort_col='label', overwrite=True)

    if DEBUG:           
        print("Calculating statistics ...")
        clusters = (hbclusters
        .groupby('label')
        .agg(F.count(F.lit(1)).alias('count'))
        .where(F.col('count') >= min_reads_per_cluster))
        
        clustered_reads = clusters.agg(F.sum('count')).collect()[0][0]
        # number of clusters
        num_clusters = clusters.count()
        print('{:,d}'.format(num_clusters) + " clusters formed.")
        print('{:,d}'.format(clustered_reads) + " reads are in clusters with at least " + str(min_reads_per_cluster) + " reads.")     

    if key_file:
        # print out estimated accuracy if reference/keys provided
        print("Estimating clustering accuracy ...")
        vertex_df = reduce(DataFrame.unionByName, [s['reads'] for s in (samples+hybrid_samples)]).select('id')
        read_ref_df = get_ref_key(spark, key_file, vertex_df)  
        get_cluster_accuracy(read_ref_df, hbclusters, showTop=20, min_reads_per_cluster=min_reads_per_cluster) 
      
        
 

    