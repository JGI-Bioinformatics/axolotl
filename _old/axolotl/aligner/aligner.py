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

def seq_align(seqfile, aligner, max_reads=0, format='fa'):
    """Call an external aligner to align reads.

    :param seqfile: sequence dataframe with at least two columns, 'name', 'seq' 
    :type seqfile: pyspark dataframe
    :param aligner: aligner string, e.g. '/dbfs/exec/minimap2 -a -K0 /dbfs/exec/test/MT-human.fa -'
    :type aligner: string
    :param max_reads: maximum reads to align, default all(0)
    :type max_reads: int, optional
    :param format: read formats ('fa' or 'fq'), default 'fa'
    :type format: string, optional

    :rtype: pyspark dataframe
    :return:  mapping information contained in SAM except 'seq' and 'qual', no headers

    .. notes:: https://samtools.github.io/hts-specs/SAMv1.pdf

    """
    ALIGNMENT_SCHEMA = "`qname` STRING, `flag` INT, `tname` STRING, `pos` LONG, `mapq` INT, `cigar` STRING, `rnext` STRING, `pnext` LONG, `tlen` LONG"
    spark = SparkSession.getActiveSession()
    if max_reads >0 :
        reads = spark.read.parquet(seqfile).limit(max_reads)
    else:
        reads = spark.read.parquet(seqfile)
    # make fasta or fastq format file
    if format == 'fa':
        reads = reads.withColumn('f', F.concat_ws('', F.array(F.lit('>'), F.concat_ws('\n', 'name', 'seq')), F.lit('\n'))).select('f')
    elif format == 'fq':
        reads = ( reads
            .withColumn('f', F.concat_ws('', F.array(F.lit('@'), F.concat_ws('\n', 'name', 'seq', '+', 'qual')), F.lit('\n')))
            .select('f')
        )
    else:
        return spark.createDataFrame([], ALIGNMENT_SCHEMA)

    aligned = (reads
            .rdd
            .map(lambda x: x[0]) # only take fasta string, ignore header
            .pipe(aligner)
            .filter(lambda x: x[0] != '@') # skip sequence headers
            .map(lambda x: x.split('\t')[0:10])
    ) 
    return spark.createDataFrame(aligned, schema=ALIGNMENT_SCHEMA) 