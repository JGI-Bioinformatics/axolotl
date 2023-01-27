"""
statistics related functions

# by zhong wang @lbl.gov

This module provides:

1) assembly statistics:
    largest contig
    total size
    N50
    N90

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def assembly_size_stat(seqfile, min_size=500):
    """
    get basic statistics of an assembly/set of reads
    """
    spark = SparkSession.getActiveSession()
    assembly = (spark.read.parquet(seqfile)
        .withColumn('length', F.length('seq'))
        .filter(F.col('length')>=min_size)
        .withColumn('cumsum', F.expr('sum(length) over (order by length)'))
    )
    max_contig_size = assembly.select(F.max('length')).collect()[0]['max(length)']
    print("Largest contig/scaffold: {:,}".format(max_contig_size))
    total_contig_size = assembly.select(F.max('cumsum')).collect()[0]['max(cumsum)']
    print("Total assembly size: {:,}".format(total_contig_size))
    n50 = assembly.filter(F.col('cumsum') >= total_contig_size*.5).select(F.min('length')).collect()[0]['min(length)']
    print("N50: {:,}".format(n50))
    n90 = assembly.filter(F.col('cumsum') >= total_contig_size*.1).select(F.min('length')).collect()[0]['min(length)']
    print("N90: {:,}".format(n90))
    print("Contig/Scaffold size statistics:")
    return assembly.select('length').summary() 