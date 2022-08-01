'''
This module provides:
  GC% of each sequence in the fastq file
  Average read quality of reads in the fastq file
  Sequence Filteration based on read quality.
  Subsampling result for Python plotting
'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
spark.udf.registerJavaFunction("getGC", "org.jgi.spark.udf.gcPercent", DoubleType())
spark.udf.registerJavaFunction("meanQuality", "org.jgi.spark.udf.meanQuality", DoubleType()) 
def reads_stats(reads, fraction=1.0, seed=1234, withReplacement=False, show=True):
    """
    output reads stats: length, GC_percentage, quality 
    """
    for col in ['id', 'seq', 'qual']:
        assert (col in reads.columns), col+" column not in reads!" 

    sql_cmd = """SELECT id, name, qual, seq, getGC(seq) as gc_percent, meanQuality(qual) as mean_qual from reads"""
    if fraction < 1.0:
        reads = reads.sample(withReplacement,fraction,seed)
    reads.createOrReplaceTempView('reads')   
    reads = spark.sql(sql_cmd)  
    reads = (reads
                .withColumn('length',F.length('seq'))
                .withColumn('N_count',F.col('length')-F.length(F.regexp_replace(F.col('seq'),'N','')))
            )
    if show:
      # print length statistics
      print("Reads quality statistics:")
      reads.select('length', 'N_count', 'mean_qual', 'gc_percent').summary().show()
      reads.select('length', 'N_count', 'mean_qual', 'gc_percent').to_pandas_on_spark().to_pandas().hist(bins=20);
      return
    return reads

def reads_filter(reads, maxNs=2, minQual=30):
    """
    filter reads based on its statistics
    """
    if ('N_count' not in reads.columns) or ('mean_qual' not in reads.columns):
        reads = reads_stats(reads, fraction=1.0, show=False)
    filtered = reads.where((F.col('N_count')<=maxNs) & (F.col('mean_qual')>=minQual))
    print("Reads %d were retained out of %d total reads." % (filtered.count(), reads.count()))
    return filtered