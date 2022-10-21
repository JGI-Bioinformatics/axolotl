"""
parquet format conversion tools 

# by zhong wang @lbl.gov

This module provides:
1) data format coversion:
    dataframe -> parquet

TODO:

"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def save_parquet(df, output_file, sort_col='', overwrite=True):
    """
    save dataframe as parquet, by default sorted to reduce file size and optimize loading
    
    """
    if sort_col and overwrite:
        df.sort(sort_col).write.mode("overwrite").parquet(output_file)
        return
    if overwrite:
        df.write.mode("overwrite").parquet(output_file)
        return  
    df.parquet(output_file)

def seq_to_fastq(seq_file, fastq_file, overwrite=False):
    """Convert seq to fastq format. Sorted by name so pairs will be together

    :param fastq_file: fastq output file, required 
    :type fastq_file: string
    :param seq_file: seq input file, required
    :type seq_file: string
    :param overwrite: whether or not overwrite existing file, optional, default False
    :type overwrite: bool 
    """
    spark = SparkSession.getActiveSession()
    input_data = (spark
        .read
        .parquet(seq_file)
        .withColumn('+', F.lit('+'))
        .select('name', 'seq', '+', 'qual')
        .sort('name', ascending=True)
    )
    # check if name begins with '@'
    if input_data.take(1)[0]['name'][0] != '@':
        input_data = input_data.withColumn('name', F.concat_ws('', F.array(F.lit('@'), F.col('name'))))
    if overwrite:
        input_data.write.mode('overwrite').csv(fastq_file,header=None, sep='\n')
    else:
        input_data.write.csv(fastq_file,header=None, sep='\n')

def seq_to_fasta(seq_file, fasta_file, overwrite=False):
    """Convert seq to fasta format.

    :param fasta_file: fastq output file, required 
    :type fasta_file: string
    :param seq_file: seq input file, required
    :type seq_file: string
    :param overwrite: whether or not overwrite existing file, optional, default False
    :type overwrite: bool 
    """
    spark = SparkSession.getActiveSession()
    input_data = (spark
        .read
        .parquet(seq_file)
        .select('name', 'seq')
    )
    # check if name begins with '>'
    if input_data.take(1)[0]['name'][0] != '>':
        input_data = input_data.withColumn('name', F.concat_ws('', F.array(F.lit('>'), F.col('name'))))
    if overwrite:
        input_data.write.mode('overwrite').csv(fasta_file,header=None, sep='\n')
    else:
        input_data.write.csv(fasta_file,header=None, sep='\n')