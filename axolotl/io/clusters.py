"""
read cluster related Input/Output functions

# by zhong wang @lbl.gov

This module provides:
1) sequence read format coversion:
    read clusters -> fasta
    bin -> clusters

TODO:
expand capability to work with non-databricks systems

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


def cluster_to_fasta(clusters, 
                    reads, 
                    output_prefix, 
                    pairs=False, 
                    topclusters=0, 
                    min_reads_per_cluster=2, 
                    singletons=True
                    ):
    """
    make individual fasta files from clusters for downstream assembly
    clusters is a dataframe with at least two columns: label<long>, id<long>
    reads is a dataframe with at least three columns: id<long>, name<long>, seq<long>
    """
    if pairs:
        # reads from two equal length read, with N in middle
        # read1: 1, read_len; read2: read_len+2, read_len
        read_len = int(len(str(reads['seq'].take(1)))/2)
        reads = (reads
        .withColumn(
                'fa', 
                F.concat(
                    F.lit('>'), 
                    F.col("name"), 
                    F.lit("_1\n"), 
                    F.substring('seq', 1, read_len), 
                    F.lit('\n'),
                    F.lit('>'), 
                    F.col("name"), 
                    F.lit("_2\n"), 
                    F.substring('seq', read_len+2, read_len), 
                    F.lit('\n')
                )
            )
        )                
    else:
        reads = (reads
        .withColumn(
            'fa', 
            F.concat(
                F.lit('>'), 
                F.col("name"), 
                F.lit("\n"), 
                F.col('seq'), 
                F.lit('\n')
                )
            )
        )


    clusters = (clusters
    .join(reads, on='id', how='left')
    .groupby('label')
    .agg(F.count(F.lit(1)).alias('count'), F.collect_list('fa').alias('fa'))
    ).cache()

    if singletons:
        single = clusters.where(F.col('count') < min_reads_per_cluster)
        single.select('fa').write.format('csv').options('header', 'false').save(output_prefix + '_singletons.fa')

    clusters = clusters.where(F.col('count') >= min_reads_per_cluster)
    if topclusters>0: # only output these many clusters
        clusters = clusters.sort('count', ascending=False).filter(F.col('count').between(1,topclusters))

    # write each row to a separate file
    (clusters
    .select('fa')
    .repartition(clusters.count())
    .write
    .mode('overwrite')
    .option("quote", "\u0000")
    .csv(output_prefix + '_clusters.fa', header=None)
    )

def metabat_bin_to_cluster(metabat_bin_bath):
    """
    format metabat bins to cluster format:
    label, name
    """
    from functools import reduce
    from pyspark.sql import DataFrame
    spark = SparkSession.getActiveSession()
    dbutils = get_dbutils(spark)

    clusters = []
    bins = dbutils.fs.ls(metabat_bin_bath)
    for b in bins:
        if b.name[-3:] == '.fa':
            label = b.name.split('.')[1]
            bin = fasta_to_seq(b.path, '').select('name').withColumn('label', F.lit(label) )
            clusters.append(bin)
    return reduce(DataFrame.unionByName, clusters)    