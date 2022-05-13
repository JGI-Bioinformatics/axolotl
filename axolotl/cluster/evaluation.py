"""

# Produce clustering metrics for both local and global clusters
# by zhong wang @lbl.gov

# testing 
from pyspark.sql.functions import *
from pyspark.sql.types import *

min_reads_per_cluster=10 # clusters with fewer numbers will be ignored
output_prefix = 'dbfs:/mnt/share.jgi-ga.org/contamination/zymo/pbio-2296.21123.ccs.filter'
key_file = 'dbfs:/mnt/share.jgi-ga.org/contamination/zymo/zymo_keys.csv'

samples, total_reads = load_samples(output_prefix + '_samples_meta.json', key='all')
read_ref_df = get_ref_key(key_file, samples[0]['reads'])  
lclusters = spark.read.parquet(output_prefix+'_localclusters')
get_cluster_accuracy(read_ref_df, lclusters, showTop=20, min_reads_per_cluster=min_reads_per_cluster) 

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

from sparc.utils import *
import pandas as pd
from functools import reduce

SEQ_SCHEMA = StructType([StructField("id", LongType(), True), StructField("Name", StringType(), True), StructField("Seq", StringType(), True)])
# SEQ_SCHEMA = "`id` LONG, `Name` STRING,  `Seq` STRING" # newer format

def get_ref_key(key_file, reads_df):
    # calculate QC if reference files are provided
    # key_file has two columns, rid->reference id, id->read_id
    # read_df must have read_id
    spark = SparkSession.getActiveSession()
    RSEQ_SCHEMA = StructType([StructField("rid", IntegerType(), True), StructField("id", LongType(), True)])
    read_ref_df = spark.read.format('csv').options(sep='\t').option("header",True).schema(RSEQ_SCHEMA).load(key_file)
    read_ref_df = (read_ref_df
                    .join(reads_df, on='id', how='inner')
                    .select('id', 'rid')
                )
    print('{:,d}'.format(read_ref_df.count()) + ' reads have ' + '{:,d}'.format(len(read_ref_df.select('rid').distinct().collect())) + ' known labels')
    RSEQ_SCHEMA = StructType([StructField("Name", StringType(), True), StructField("rid", StringType(), True)])
    return read_ref_df


def get_cluster_accuracy(read_ref_df, clusters, showTop=20, min_reads_per_cluster=10):
    """
    estimate clustering accuracy
    clusters is a dataframe: <label, Long> <id, Long>
    read_ref_df is a dataframe: <id, Long> <rid, Long>
    """
    # cluster label to ref id
    read_ref_df = read_ref_df.join(clusters, on='id', how='left').fillna('-1')

    mcp_udf = F.udf(lambda x: most_common_percentage(x), FloatType())
    clusters = (read_ref_df
                .where(F.col('label') > -1)
                .groupby('label')
                .agg(F.count(F.lit(1)).alias('cluster_size'), F.collect_list('rid').alias('rids'))
                .where(F.col('cluster_size') >=min_reads_per_cluster)
                .withColumn('purity', mcp_udf('rids'))
            ).cache()
    clusters.select('purity').summary().show()
    clusters.agg({'cluster_size': 'sum'}).show()

    if showTop > 0:
        pcommon_udf = F.udf(lambda x: print_common(x), StringType()) 
        (clusters
            .withColumn('Top_3_components', pcommon_udf('rids'))
            .drop('rids')
            .sort(F.desc('cluster_size')).show(showTop, truncate=False)
        )
    # for completeness calculation, first match each genome to the largest cluster, then calculate the percent of genome coverage of that cluster
    # completeness = (clusters that contain the most reads from a genome)/(total reads from a genome)
    completeness = (read_ref_df
                    .where(F.col('label') > -1)
                    .groupby('rid')
                    .agg(F.count(F.lit(1)).alias('ref_size'), F.collect_list('label').alias('labels'))
                    .withColumn('completeness', mcp_udf('labels'))
                    .na.fill({'completeness':0.0})
                    )
    if showTop > 0:
        completeness.select('rid', 'ref_size', 'completeness').sort('ref_size', ascending=False).show(showTop)                
    completeness.select('completeness').summary().show()

    return 0

    