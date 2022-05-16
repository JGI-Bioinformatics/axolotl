"""
aligner related Input/Output functions

# by zhong wang @lbl.gov

This module provides:

1) sequence mapping

TODO:
more aligner, more in/out options

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def seq_align(seqfile, aligner, max_reads=0):
    """
    call external aligner to align reads, return the mapping information 'qname', 'flag', 'tname', 'pos', 'mapq'
    example: aligner='/dbfs/exec/minimap2 -a -K0 /dbfs/exec/test/MT-human.fa -'
    """
    spark = SparkSession.getActiveSession()
    if max_reads >0 :
        reads = spark.read.parquet(seqfile).limit(max_reads)
    else:
        reads = spark.read.parquet(seqfile)
    # make fasta format file
    reads = reads.withColumn('fa', F.concat_ws('', F.array(F.lit('>'), F.concat_ws('\n', 'name', 'seq')), F.lit('\n'))).select('fa')
    return (reads
            .rdd
            .map(lambda x: x[0]) # only take fasta string, ignore header
            .pipe(aligner)
            .filter(lambda x: x[0] != '@') # skip sequence headers
            .map(lambda x: x.split('\t')[0:5])
            .toDF()
            .toDF(*['qname', 'flag', 'tname', 'pos', 'mapq'])

    )  