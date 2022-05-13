"""
reads related Input/Output functions

# by zhong wang @lbl.gov

This module provides:
1) sequence read format coversion:
    fasta -> seq (parquet)
    fastq -> seq (parquet)
    read clusters -> fasta

2) sequence mapping

3) assembly statistics:
    largest contig
    total size
    N50
    N90



"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os,subprocess

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


# popular sequence format
SEQ_SCHEMA = "`id` LONG, `name` STRING,  `seq` STRING" # seq format
PFASTQ_SCHEMA = "`name1` STRING, `seq1` STRING,  `plus1` STRING, `qual1` STRING, `name2` STRING, `seq2` STRING,  `plus2` STRING, `qual2` STRING" # paired fastq format
FASTQ_SCHEMA = "`name` STRING, `seq` STRING,  `plus` STRING, `qual` STRING" # fastq format
PFASTA_SCHEMA = "`name1` STRING, `seq1` STRING,  `name2` STRING, `seq2` STRING" # pfasta format
FASTA_SCHEMA = "`name` STRING, `seq` STRING" # fasta format

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
    assembly.select('length').summary().show()    

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