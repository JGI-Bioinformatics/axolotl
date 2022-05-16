"""
reads related Input/Output functions

# by zhong wang @lbl.gov

This module provides:
1) sequence read format coversion:
    fasta -> seq (parquet)
    fastq -> seq (parquet)
    fasta -> csv (csv), non-spark, slow
    fastq -> csv (csv), non-spark, slow

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

def fastq_to_csv(input_fastq_file, text_file, pairs=True, overwrite=False):
    """convert fastq to csv format"""
    if os.path.isfile(text_file) and ~overwrite:
        print(" Target file exist, set overwrite=True to overide.")
        return
    paste = ' paste - - - - -d "\t" '
    if pairs:
        paste = ' paste - - - - - - - - -d "\t" '
    # convert fastq to one line
    if input_fastq_file.endswith(".gz"):
        cmd = 'gzip -cd {} | {} > {}'.format(input_fastq_file, paste, text_file)        
    else:
        cmd = 'cat {} | {} > {}'.format(input_fastq_file, paste, text_file)
    cmd = cmd.split(' ')
    subprocess.run(cmd, shell=True)
    print("... converted to one line file: {}".format(text_file))

def fasta_to_csv(input_fasta_file, text_file, pairs=True, overwrite=False):
    """convert fasta to csv format"""
    if os.path.isfile(text_file) and ~overwrite:
        print(" Target file exist, set overwrite=True to overide.")
        return 
    paste = ' paste - - -d "\t" '
    if pairs:
        paste = ' paste - - - - -d "\t" '    
    # convert fasta to one line
    if input_fasta_file.endswith(".gz"):
        cmd = 'gzip -cd {} | {} > {}'.format(input_fasta_file, paste, text_file)       
    else:
        cmd = 'cat {} | {} > {}'.format(input_fasta_file, paste, text_file)
    cmd = cmd.split(' ')
    subprocess.run(cmd, shell=True)
    print("... converted to one line file: {}".format(text_file))

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

def fastq_to_seq(output_pq_file, read1, read2=None, joinpair=False, overwrite=True):
    """
    convert fastq file to seq format (parquet)
    seqfile has: id, name, seq, qual
    read1 can be a single unpaired file, or the first read of the pair (required)
    for paired reads, if joinpair==True, join the two reads by a 'N'
    """
    spark = SparkSession.getActiveSession()
    input_data = (spark.read.text(read1, lineSep='@')
     .filter(F.length('value')>1)
    )

    if read2:
        input_data.union(spark.read.text(read2, lineSep='@')
            .filter(F.length('value')>1)
        )
        
    input_data = (input_data
        .withColumn('name', F.split('value', '\n').getItem(0))
        .withColumn('seq', F.split('value', '\n').getItem(1))
        .withColumn('qual', F.split('value', '\n').getItem(3))
        .withColumn("id", F.monotonically_increasing_id())
        .select('id', 'name', 'seq', 'qual')
    )

    if read2:
        input_data = input_data.sort('name')
        
    if joinpair:
        # assuming the two reads end with /1 and /2
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

def clean_up_read(seq):
        """
        replace non AGCT bases to N
        """
        return re.sub("[^AGCT]+", "N", seq)
clean_up_read_udf = F.udf(lambda x: clean_up_read(x), StringType())