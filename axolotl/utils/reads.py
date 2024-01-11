"""
reads related functions

This utility provides:
1) read format coversion:
    fasta <-> parquet
    fastq <-> parquet
    SAM -> parquet
2) read statistics:
   GC% of each sequence in the fastq file
   Average read quality of reads in the fastq file
   Sequence Filteration based on read quality.
   Subsampling result for Python plotting
TODO:
Add more support for SAM/BAM formats

"""
from axolotl.utils.spark import get_spark_session_and_context
spark, sc = get_spark_session_and_context()

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, LongType, DoubleType

# register java udfs
spark.udf.registerJavaFunction("getGC", "org.jgi.axolotl.udfs.gcPercent", DoubleType())
spark.udf.registerJavaFunction("meanQuality", "org.jgi.axolotl.udfs.meanQuality", DoubleType())
spark.udf.registerJavaFunction("jkmerudf", "org.jgi.axolotl.udfs.jkmer", ArrayType(LongType())) 

import os,subprocess
import re
import random
import string

# popular sequence format
SEQ_SCHEMA = "`id` LONG, `name` STRING,  `seq` STRING" # seq format
PFASTQ_SCHEMA = "`name1` STRING, `seq1` STRING,  `plus1` STRING, `qual1` STRING, `name2` STRING, `seq2` STRING,  `plus2` STRING, `qual2` STRING" # paired fastq format
FASTQ_SCHEMA = "`name` STRING, `seq` STRING,  `plus` STRING, `qual` STRING" # fastq format
PFASTA_SCHEMA = "`name1` STRING, `seq1` STRING,  `name2` STRING, `seq2` STRING" # pair reads fasta format
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
          ps = subprocess.Popen(['cat', input_fastq_file], stdout=subprocess.PIPE)
          subprocess.call(paste, stdin=ps.stdout, stdout=f) 
          ps.wait()

def fastq_to_pq(read1, read2=None, output_pq_file='', pairs=True, joinpair=False, overwrite=False):
    """Convert fastq file to parquet via a csv intermediate file

    :param read1: input fastq file, or read1 file (paired inputs), required
    :type read1: string
    :param read2: read2 file (paired reads), optional
    :type read2: string
    :param output_pq_file: pq format output (id, name, seq, qual). If empty string, return the dataframe 
    :type output_pq_file: string
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
    input_data = fastq_to_df(read1)
    if read2:
        input_data = (input_data 
            .union(fastq_to_df(read2))
            .sort('name')
            .withColumn("id", F.monotonically_increasing_id()) # replace ids with new ones to avoid conflict
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

def fasta_to_pq(input_fasta_file, output_pq_file='', overwrite=False):
    """
    convert fasta file to parquet
    result dataframe has: id, name, seq
    return a dataframe if no output_pq specified
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

def reads_to_df(read1, read2=None, format='fq', max_reads=0, joinpair=False):
    """
    import reads with allowed formats: fa, fq, pq
    pairs are indicted with a 'p': pfa, pfq
    return reads dataframe
    """

    if read2:
        print('Inputs are paired fastq format: %s <-> %s' % (read1, read2))
        read_df = fastq_to_pq(output_pq_file='', read1=read1, read2=read2, joinpair=joinpair)
    else:
        if format == 'fa':
            print('%s is fasta format' % read1)
            read_df = fasta_to_pq(read1, output_pq_file='')
        elif format == 'fq':
            print('%s is fastq or interleaved paired fq format' % read1)
            read_df = fastq_to_pq(output_pq_file='', read1=read1, joinpair=joinpair)
        elif format == 'pq':
            print('%s is parquet/seq format' % read1)
            read_df = spark.read.parquet(read1)
    if max_reads >0:
        return read_df.limit(max_reads)
    else:
        return read_df

def fastq_to_df(input_fq):
    """
    take a fastq file, return a readDF with read index
    This method doesn't go through a csv intermediate file, it may have poorer performance because the groupBy operation.
    """
    fastqDF = (sc
        .textFile(input_fq)
        .zipWithIndex()
        .map(lambda x: (x[0], int(x[1]/4), x[1]%4))
        .groupBy(lambda x: x[1])
        .map(lambda x: sorted(list(x[1]), key=lambda y:y[2], reverse=False))
        .map(lambda x: (x[0][1], x[0][0], x[1][0], x[2][0], x[3][0]))
    ).toDF(['id', 'name', 'seq', 'plus', 'qual'])
    return fastqDF

def sam_to_pq(input_sam_file, output_pq_file='', overwrite=False):
    """
    convert unaligned sam file to parquet
    only take name, seq
    return a dataframe if no output_pq specified
    """
    reads_df = spark.read.csv(sam_file, sep='\t', header=False, comments="@")
    # only use the qname and seq columns, rename to 'id' and 'seq'
    reads_df = (reads_df
        .select(*(reads_df.columns[0,9]))
        .toDF(['name', 'seq'])
        .withColumn("id", F.monotonically_increasing_id()) 
    )
    if output_pq_file == '':
        return reads_df
    else:
        if overwrite:
            reads_df.write.mode("overwrite").parquet(output_pq_file)
        else:
            reads_df.write.parquet(output_pq_file) 

def clean_up_read(seq):
        """
        replace non AGCT bases to N
        """
        return re.sub("[^AGCT]+", "N", seq)
clean_up_read_udf = F.udf(lambda x: clean_up_read(x), StringType())

def pq_to_fastq(pq_file, fastq_file, overwrite=False):
    """Convert parquet (pq) to fastq format. Sorted by name so pairs will be together. 
    If one file is desired, concat all the resulting partitions into one file

    :param fastq_file: fastq output file, required 
    :type fastq_file: string
    :param pq_file: pq input file, required
    :type pq_file: string
    :param overwrite: whether or not overwrite existing file, optional, default False
    :type overwrite: bool 
    """
    input_data = (spark
        .read
        .parquet(pq_file)
        .withColumn('+', F.lit('+'))
        .select('name', 'seq', '+', 'qual')
        .sort('name', ascending=True)
    )
    # add '@' to the seq name if necessary
    if input_data.take(1)[0]['name'][0] != '@':
        input_data = input_data.withColumn('name', F.concat_ws('', F.array(F.lit('@'), F.col('name'))))
    if overwrite:
        input_data.write.mode('overwrite').csv(fastq_file,header=None, sep='\n')
    else:
        input_data.write.csv(fastq_file,header=None, sep='\n')

def pq_to_fasta(pq_file, fasta_file, overwrite=False):
    """Convert parquet to fasta format. If one file is desired, concat all the resulting partitions into one file

    :param fasta_file: fastq output file, required 
    :type fasta_file: string
    :param pq_file: parquet input file, required
    :type pq_file: string
    :param overwrite: whether or not overwrite existing file, optional, default False
    :type overwrite: bool 
    """
    input_data = (spark
        .read
        .parquet(pq_file)
        .select('name', 'seq')
    )
    # add '>' to the seq name if necessary
    if input_data.take(1)[0]['name'][0] != '>':
        input_data = input_data.withColumn('name', F.concat_ws('', F.array(F.lit('>'), F.col('name'))))
    if overwrite:
        input_data.write.mode('overwrite').csv(fasta_file,header=None, sep='\n')
    else:
        input_data.write.csv(fasta_file,header=None, sep='\n')

def reads_stats(reads, fraction=0.01, seed=1234, withReplacement=False, show=False):
    """
    given a read dataframe (with columns id, seq and qual)
    output reads stats: length, N_count, GC_percentage, mean_quality 
    change show=True to plot a histogram
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

    # kmers in the reference file
    kmers_ref = count_kmers(reference, k=k, m=m, min_kmer_count=min_kmer_count)
    # Kmers in the sample file
    kmers_input = reads_to_kmer(reads, k=k, m=m).select('id', 'kmer')

    contaminated_reads = (kmers_input
                            .join(kmers_ref, on='kmer', how='inner')
                            .select('id')
    )

    clean = reads.join(contaminated_reads, on='id', how='left_anti')


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