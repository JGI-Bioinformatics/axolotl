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
from axolotl.core.krmapper import *
from axolotl.io.parquetio import *
import random
import string

spark = SparkSession.builder.getOrCreate()
spark.udf.registerJavaFunction("getGC", "org.jgi.spark.udf.gcPercent", DoubleType())
spark.udf.registerJavaFunction("meanQuality", "org.jgi.spark.udf.meanQuality", DoubleType())
spark.udf.registerJavaFunction("jkmerudf", "org.jgi.spark.udf.jkmer", ArrayType(LongType())) 

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

def quality_filter(reads, maxNs=2, minQual=30):
    """
    filter reads based on its statistics
    """
    if ('N_count' not in reads.columns) or ('mean_qual' not in reads.columns):
        reads = reads_stats(reads, fraction=1.0, show=False)
    filtered = reads.where((F.col('N_count')<=maxNs) & (F.col('mean_qual')>=minQual))
    print("Reads %d were retained out of %d total reads." % (filtered.count(), reads.count()))
    return filtered
    
def contamination_filter(outfile, reads, reference, k=31, minimizer=False, min_kmer_count=0, singleton=True):
    """
    :param outfile: output file
    :type outfile: string
    :param reads: input data
    :type reads: DataFrame
    :param reference: known contaminant
    :type reference: DataFrame
    :param k: kmer length
    :type  k: int
    :param minimizer: whether to use minimizer
    :type minimizer: bool
    :param min_kmer_count: remove kmers with low occurance
    :type min_kmer_count: int
    :return cleaned reads
    :return type: dataframe or none
    """
    m = int(k/2) + 1 if minimizer else k
    n = 0

    # kmers in the reference file
    kmers_ref = reads_to_kmersu(reference, k=k, m=m, n=n).select('id', 'sid', 'kmerlist')
    kmers_ref = kmer_filter(kmers_ref, 0).select('kmer', F.explode('Ids').alias('id'))
    # Kmers in the sample file
    kmers_input = reads_to_kmersu(reads, k=k, m=m, n=n).select('id', 'sid', 'kmerlist')
    kmers_input = kmer_filter(kmers_input, min_kmer_count).select('kmer', F.explode('Ids').alias('rid'))

    contaminated_kmers = (kmers_input
                            .join(kmers_ref, on='kmer', how='inner')
                            .select('rid')
    )

    clean = (reads
                .join(contaminated_kmers,reads['id'] == contaminated_kmers['rid'],'left_anti')
                .drop('rid')
    )

    if not singleton:
        # drop singletons
        # assuming the two reads end with /1 and /2
        # still works if the names of the pair are the same
        doublets = (clean
            .withColumn('pname', F.split('name', '/').getItem(0))
            .groupby('pname')
            .agg(F.count('pname'))
            .where(F.col('pname')>1)
        )
        clean = clean.join(doublets, clean['name'] == doublets['pname'], 'inner')
    print("Reads %d were retained out of %d total reads." % (clean.count(), reads.count()))
    if outfile == '':
        return clean
    else:
        save_parquet(clean,outfile)

