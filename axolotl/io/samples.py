"""
# Genomics sample class
# By zhong wang @lbl.gov

spark = SparkSession.builder.getOrCreate()

from sparc.samples import *

input_seq = 's3://share.jgi-ga.org/gis20mock/illumina.seq'
sample1 = Sample(sampleId=0, sampleName='test')
sample1.import_sequences(read1=[input_seq], read2=[], format='seq', max_files=0, max_reads=0, remove_non_alphabet=False)
sample1.save(datapath='s3://share.jgi-ga.org/gis20mock/illumina_', overwrite=False)    

# load an existing sample
sample1 = Sample(datapath='s3://share.jgi-ga.org/gis20mock/illumina_')

# print out sample info
sample1.info()

"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
import os, re
from sparc.reads import *

def clean_up_read(seq):
        """
        replace non AGCT bases to N
        """
        return re.sub("[^AGCT]+", "N", seq)
clean_up_read_udf = F.udf(lambda x: clean_up_read(x), StringType())

def allowed_extensions(filename):
    """
    allowed file extensions
    """
    allowed = ['fa', 'fq', 'seq']
    if filename.split('.')[-1] == 'gz':
        filename = '.'.join(filename.split('.')[0:-1])
    if filename.split('.')[-1] in allowed:
        return True
    else:
        return False

def reads_to_df(read1, read2=None, format='fq', max_reads=0, joinpair=False):
    """
    import reads with allowed formats: fa, fq, seq
    pairs are indicted with a 'p': pfa, pfq
    return reads dataframe
    """
    spark = SparkSession.getActiveSession()

    if read2:
        print('Inputs are paired fastq format: %s <-> %s' % (read1, read2))
        read_df = fastq_to_seq(output_pq_file='', read1=read1, read2=read2, joinpair=joinpair)
        return read_df.limit(max_reads)

    if format == 'fa':
        print('%s is fasta format' % read1)
        read_df = fasta_to_seq(read1, output_pq_file='')
    elif format == 'fq':
        print('%s is fastq or interleaved paired fq format' % read1)
        read_df = fastq_to_seq(output_pq_file='', read1=read1, joinpair=joinpair)
    elif format == 'seq':
        print('%s is parquet/seq format' % read1)
        if max_reads >0:
            read_df = spark.read.parquet(read1)
        else:
            read_df = spark.read.parquet(read1)

    return read_df.limit(max_reads)

class Sample:
    def __init__(self,  *datapath, **kwargs):
        """
        sampleId is a unique integer
        sampleName is a unique name
        filePath is the path of the sequence file, or a directory containing the sequencing files
        """
        spark = SparkSession.getActiveSession()
        metadata = spark.read.json(datapath + 'meta.json').collect().asDict()
        for key in metadata:
            setattr(self, key, metadata[key])
        if self.readPath:
            self.reads = spark.read.parquet(self.readPath)
        if self.kmerPath:
            self.kmers = spark.read.parquet(self.kmerPath) 
        for key in kwargs: # if not new object, will overwrite the loaded values
            setattr(self, key, kwargs[key])

    def update_or_add(self, attr, value):
        # update an existing attribute or add a new one
        if hasattr(self, attr):
            self.attr = value
        else:
            setattr(self, attr, value)

    def info(self):
        # print out sample info
        print("Sample ID:%d" % self.id) 
        print("Sample Name:%s" % self.name)
        print("Sample dataPath:%s" % self.dataPath)
        if self.reads:
            print("Sample readCount:%d" % self.readCount)
        else:
            print("Sample doesn't contain any reads.")

        if self.kmers:
            print("Sample has kmer calcuated.")  

    def import_sequences(self, read1, read2=[], format='fq', joinpair=False, max_files=0, max_reads=0, remove_non_alphabet=False):
        """
        import sequences
        read1 can be a single file, or a single directory containing multiple files from the same sample
        if pairs are in seperate files, they need to be provided as two lists: read1=['L0-r1', 'L1-r1'], read2=['L0-r2', 'L1-r2']
        for pairs, setting joinpair=True will cause the two reads are joined by a 'N'
        only .seq, .fa, or .fq files are read, gzip is allowed
        """  
        files = []
        for input_seq in read1:
            if input_seq[0:5] == 's3://':
                files += process_aws_files(input_seq, max_files=max_files)
            elif input_seq[0:5] == 'gs://':
                files += process_gcs_files(input_seq, max_files=max_files)                  
            elif input_seq[0:6] == 'dbfs:/':
                files += process_dbfs_files(input_seq, max_files=max_files)           
            else:
                files += process_unix_files(input_seq, max_files=max_files)
        if read2:
            files2 = []               
            for input_seq in read2:
                if input_seq[0:5] == 's3://':
                    files2 += process_aws_files(input_seq, max_files=max_files)
                elif input_seq[0:5] == 'gs://':
                    files2 += process_gcs_files(input_seq, max_files=max_files)                  
                elif input_seq[0:6] == 'dbfs:/':
                    files2 += process_dbfs_files(input_seq, max_files=max_files)           
                else:
                    files2 += process_unix_files(input_seq, max_files=max_files)
        else:
            files2 = [] 
        self.update_or_add('read1', files)
        self.update_or_add('read2', files2)
        self.update_or_add('originalFormat', format)
        self.update_or_add('joinPair', joinpair)
        self.update_or_add('removeNonAlphabet', remove_non_alphabet)
        # load reads from multiple files
        # fix ids so that they are unique       
        total_read_count = 0
        all_reads = []
        for i in range(len(files)):
            if len(files2) > i:
                read2 = files2[i]
            else:
                read2 = None
            read1 = files[i]
            reads = reads_to_df(read1, read2=read2, joinpair=joinpair, format=format, max_reads=max_reads)            
            # replace non alphabet [ACGT] bases to N
            reads = reads.withColumn('upper', F.upper('seq'))
            if remove_non_alphabet:
                reads = ( reads
                    .withColumn('clean', clean_up_read_udf('upper'))
                    .selectExpr("id as id", "name as name", "clean as seq")
                )
            else:
                reads = reads.selectExpr("id as id", "name as name", "upper as seq")
            reads = reads.withColumn('sid', F.lit(self.id))
                
            counts = reads.count() 
            total_read_count += counts
            print('{:,d}'.format(total_read_count) + ' reads have been imported.') 
            all_reads.append(reads)
        self.update_or_add('reads', None)
        self.update_or_add('readCount', 0)
        if len(all_reads) >1:
            # combine all reads, reset the read id
            self.reads = reduce(DataFrame.unionByName, [r.select('sid', 'name', 'seq') for r in all_reads])
            self.reads.withColumn('id', F.monotonically_increasing_id())
        else:
            self.reads = all_reads[0]           
        self.readCount = total_read_count

    def save(self, datapath=None, key='reads', overwrite=True):
        # save a sample
        spark = SparkSession.getActiveSession()
        if datapath: # default to sample's datapath
            self.dataPath = datapath
        else:
            datapath = self.dataPath
        metadata = vars(self)
        metadata.pop('reads', None)
        metadata.pop('kmers', None)
        if self.reads and (key == 'reads'): #save the read data
            readPath = datapath + 'reads.pq'
            if overwrite:
                self.reads.write.mode("overwrite").parquet(readPath)
            else:
                self.reads.write.parquet(readPath)
            metadata['readPath'] = readPath
        if self.kmers and (key == 'kmers'): #save the read data
            kmerPath = datapath + 'kmers.pq'
            if overwrite:
                self.kmers.write.mode("overwrite").parquet(kmerPath)
            else:
                self.kmers.write.parquet(kmerPath)
            metadata['kmerPath'] = kmerPath
        # save metafile
        meta_path = datapath + 'meta.json'
        spark.sparkContext.parallelize(metadata).toDF().write.mode("overwrite").json(meta_path)
     

def process_aws_files(input_seq, max_files=0):
    #
    import boto3
    bucket = input_seq.replace('s3://', '').split('/')[0]
    key = '/'.join(input_seq.replace('s3://', '').split('/')[1:])
    my_bucket = boto3.resource('s3').Bucket(bucket)
    counter = 0
    files = []
    for f in my_bucket.objects.filter(Prefix=key):
        if allowed_extensions(f.key):
            files.append('s3a://' + bucket + '/' + f.key)
            counter +=1
            if (max_files>0) and (counter >= max_files):
                break
    return files

def process_gcs_files(input_seq, max_files=0):
    from google.cloud import storage
    bucket = input_seq.replace('gs://', '').split('/')[0]
    key = '/'.join(input_seq.replace('gs://', '').split('/')[1:])
    client = storage.Client()
    files = []
    counter = 0
    for f in client.list_blobs(bucket, prefix=key):
        if allowed_extensions(f.name):
            files.append('gs://' + bucket + '/' + f.name)
            counter +=1
            if (max_files>0) and (counter >= max_files):
                break    
    return files

def process_dbfs_files(input_seq, max_files=0):
    from pyspark.dbutils import DBUtils
    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    counter = 0
    files=[]
    for f in dbutils.fs.ls(input_seq):
        if allowed_extensions(f.name):
            files.append(f.path)
            counter +=1
            if (max_files>0) and (counter >= max_files):
                break   
    return files

def process_unix_files(input_seq, max_files=0):
    counter = 0
    files = []
    for f in os.listdir(input_seq):
        if allowed_extensions(f):
            files.append('/'.join([input_seq, f]))
            counter +=1
            if (max_files>0) and (counter >= max_files):
                break  
    return files