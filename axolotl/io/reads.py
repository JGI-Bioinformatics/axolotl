"""
reads related Input/Output functions

# by zhong wang @lbl.gov

This module provides:
1) sequence read format coversion:
    fasta -> seq (parquet)
    fastq -> seq (parquet)

TODO:
Add support for SAM/BAM formats

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os,subprocess
import re

# popular sequence format
SEQ_SCHEMA = "`id` LONG, `name` STRING,  `seq` STRING" # seq format
PFASTQ_SCHEMA = "`name1` STRING, `seq1` STRING,  `plus1` STRING, `qual1` STRING, `name2` STRING, `seq2` STRING,  `plus2` STRING, `qual2` STRING" # paired fastq format
FASTQ_SCHEMA = "`name` STRING, `seq` STRING,  `plus` STRING, `qual` STRING" # fastq format
PFASTA_SCHEMA = "`name1` STRING, `seq1` STRING,  `name2` STRING, `seq2` STRING" # pfasta format
FASTA_SCHEMA = "`name` STRING, `seq` STRING" # fasta format

def fastq_to_csv(input_fastq_file, csv_file, pairs=True, overwrite=False):
    """Convert fastq to csv format.
    :param input_fastq_file: fastq input file, required 
    :type input_fastq_file: string
    :param csv_file: csv output file, required
    :type csv_file: string
    :param pairs: whether or not input fastq are paired, optional, default True
    :type pairs: bool
    :param overwrite: whether or not overwrite existing file, optional, default False
    :type overwrite: bool

    :rtype: None
    :return:  None
    """
    if os.path.isfile(csv_file) and ~overwrite:
        print(" Target file exist, set overwrite=True to overide.")
        return
    paste = 'paste - - - - -d \t'.split(' ')
    if pairs:
        paste = 'paste - - - - - - - - -d \t'.split(' ')
    # convert fastq to one line
    with open(csv_file, 'w') as f:
      if input_fastq_file.endswith(".gz"):
          ps = subprocess.Popen(['gzip', '-cd', input_fastq_file], stdout=subprocess.PIPE)
          subprocess.call(paste, stdin=ps.stdout, stdout=f)
          ps.wait()
      else:
          ps = subprocess.call( paste + [input_fastq_file], stdout=f) 
          ps.wait()

def fastq_to_seq(output_pq_file, read1, read2=None, pairs=True, joinpair=False, overwrite=True, temp_dir='/local_disk0/tmp/'):
    """Convert fastq file to seq format (parquet) via a csv intermediate file

    :param output_pq_file: seq format output (id, name, seq, qual), required. If empty string, return the dataframe 
    :type output_pq_file: string
    :param read1: input fastq file, or read1 file (paired inputs), required
    :type read1: string
    :param read2: read2 file (paired reads), optional
    :type read2: string
    :param pairs: whether or not input fastq are paired, optional, default True
    :type pairs: bool
    :param joinpair: whether or not concatnate the two reads using a 'N', optional, default False
    :type joinpair: bool
    :param overwrite: whether or not overwrite existing file, optional, default False
    :type overwrite: bool
    :param temp_dir: dirctory to store the csv file, optional
    :type temp_dir: string

    :rtype: None
    :return:  None
    """
    import random
    import string
    spark = SparkSession.getActiveSession()
    temp_file1 = temp_dir + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
    if read1[0:5] == 'dbfs:':
        read1 = read1.replace('dbfs:', '/dbfs')
    fastq_to_csv(read1, temp_file1, pairs=pairs)
    input_data = spark.read.csv('file://'+ temp_file1, sep='\t', schema=FASTQ_SCHEMA)
    if read2:
        if read2[0:5] == 'dbfs:':
            read2 = read1.replace('dbfs:', '/dbfs')
        temp_file2 = temp_dir + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        input_data = ( input_data
            .union(spark.read.csv('file://' + temp_file2, sep='\t', schema=FASTQ_SCHEMA))
            .sort('name')
        )
        
    input_data = (input_data
        .withColumn("id", F.monotonically_increasing_id())
        .select('id', 'name', 'seq', 'qual')
    )
 
    if joinpair:
        # assuming the two reads end with /1 and /2
        # still works if the names of the pair are the same
        input_data = (input_data
            .withColumn('name', F.split('name', '/').getItem(0))
            .groupby('name')
            .agg(
                F.concat_ws('N', F.collect_list('seq')).alias('seq'),
                F.concat_ws('B', F.collect_list('qual')).alias('qual'),
            )
            .withColumn("id", F.monotonically_increasing_id())
            .select('id', 'name', 'seq', 'qual')
        ) 
    if output_pq_file == '':
        return input_data
    else:
        if overwrite:
            input_data.write.mode("overwrite").parquet(output_pq_file)
        else:
            input_data.write.parquet(output_pq_file) 

def fasta_to_seq(input_fasta_file, output_pq_file, overwrite=True):
    """
    convert fasta file to seq format (parquet)
    seqfile has: id, name, seq
    """
    spark=SparkSession.getActiveSession()
    input_data = (spark.read.text(input_fasta_file, lineSep='>')
     .filter(F.length('value')>1)
     .withColumn('name', F.split('value', '\n').getItem(0))
     .withColumn('seq', F.concat_ws('', F.slice(F.split('value', '\n'), 2, 1000000)) )
     .withColumn("id", F.monotonically_increasing_id())
     .select('id', 'name', 'seq')
    )
    if output_pq_file == '':
        return input_data
    else:
        if overwrite:
            input_data.write.mode("overwrite").parquet(output_pq_file)
        else:
            input_data.write.parquet(output_pq_file) 



def clean_up_read(seq):
        """
        replace non AGCT bases to N
        """
        return re.sub("[^AGCT]+", "N", seq)
clean_up_read_udf = F.udf(lambda x: clean_up_read(x), StringType())