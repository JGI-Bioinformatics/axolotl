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
<<<<<<< Updated upstream
    return filtered
=======
    return filtered
    
def contamination_detection(reads, reference, k=31, minimizer=False, min_kmer_count=0, save_file=False, show=False):
  """
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
  :param save_file: If True, save clean sequences to fastq file, else return a DataFrame
  :type save_file: bool
  :param show: If True, print the filteration stats. 
  :type show: bool
  """
  m = int(k/2) + 1 if minimizer else k
  n = 0
  
  # kmers in the reference file
  kmers_ref = reads_to_kmersu(reference, k=k, m=m, n=n).select('id', 'sid', 'kmerlist')
  
  # filter the Kmers in the reference file using the min_kmer parameter
  # remove duplicated kmers
  filtered_kmers_ref = kmer_filter(kmers_ref, min_kmer_count)
  
  # Kmers in the sample file
  kmers_input = (reads_to_kmersu2(reads, k=k, m=m, n=n)
                .select('id', 'sid', 'pname', F.explode("kmerlist").alias('kmer')))
  
  contaminated_kmers=(kmers_input.join(filtered_kmers_ref,
                                  kmers_input['kmer'] ==filtered_kmers_ref['kmer'],'inner')
                                 .select(F.col('pname').alias('cpname')))
  
  clean=(reads.join(contaminated_kmers
                    ,reads['pname'] == contaminated_kmers['cpname'],'left_anti')
                    .select('id','sid','name','pname','seq','qual'))
  if show:
     print("Reads %d were retained out of %d total reads." % (clean.count(), reads.count()))
  if save_file:
    temp_dir='/local_disk0/tmp/'
    temp_file1 = temp_dir + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    save_parquet(clean,temp_file1)
    # return file path
    return temp_file1
  else:
    return clean
>>>>>>> Stashed changes
