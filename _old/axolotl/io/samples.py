"""
# Genomics sample class
# By zhong wang @lbl.gov

spark = SparkSession.builder.getOrCreate()

from axolotl.io.samples import *

input_seq = 's3://share.jgi-ga.org/gis20mock/illumina.seq'
sample1 = Sample(sampleId=0, sampleName='test')
sample1.import_sequences(read1=[input_seq], read2=[], format='seq', max_files=0, max_reads=0, remove_non_alphabet=False)
sample1.save(dataPath='s3://share.jgi-ga.org/gis20mock/illumina_meta.json', overwrite=False)    

# load an existing sample
sample2 = Sample()
sample2 = Sample(dataPath='s3://share.jgi-ga.org/gis20mock/illumina_meta.json')

# print out sample info
sample2.info()

"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from axolotl.io.reads import reads_to_df, clean_up_read_udf
from axolotl.io.cloudFS import get_cloud_filelist

"""
# Genomics sample class
# By zhong wang @lbl.gov

spark = SparkSession.builder.getOrCreate()

from axolotl.io.samples import *

input_seq = 's3://share.jgi-ga.org/gis20mock/illumina.seq'
sample1 = Sample(sampleId=0, sampleName='test')
sample1.import_sequences(read1=[input_seq], read2=[], format='seq', max_files=0, max_reads=0, remove_non_alphabet=False)
sample1.save(dataPath='s3://share.jgi-ga.org/gis20mock/illumina_meta.json', overwrite=False)    

# load an existing sample
sample2 = Sample()
sample2 = Sample(dataPath='s3://share.jgi-ga.org/gis20mock/illumina_meta.json')
# print out sample info
sample2.info()

"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from axolotl.io.reads import fastq_to_seq, fasta_to_seq, clean_up_read_udf
from axolotl.io.cloudFS import get_cloud_filelist

SAMPLE_SCHEMA = StructType([
  StructField("sampleId", IntegerType()),
  StructField("sampleName", StringType()),
  StructField("read1", ArrayType(StringType())),
  StructField("read2", ArrayType(StringType())),            
  StructField("originalFormat",  StringType()),
  StructField("joinPair", BooleanType()),
  StructField("removeNonAlphabet", BooleanType()),  
  StructField("readPath", StringType()),
  StructField("kmerPath", StringType()), 
  StructField("dataPath", StringType()),  
  StructField("readCount", LongType()),
  StructField("kmerCount", LongType())
])

class Sample:
    def __init__(self, **kwargs):
        """
        sampleId is a unique integer
        sampleName is a unique name
        filePath is the path of the sequence file, or a directory containing the sequencing files
        """
        for key in kwargs: # if not new object, will overwrite the loaded values
            self.__dict__.update(kwargs)

    def update_or_add(self, attr, value):
        # update an existing attribute or add a new one
        if hasattr(self, attr):
            self.attr = value
        else:
            setattr(self, attr, value)

    def load(self, dataPath=''):
        # load a saved sample
        spark = SparkSession.getActiveSession()
        try:
            metadata = spark.read.json(dataPath, schema=SAMPLE_SCHEMA).collect()[0].asDict()
            for key in metadata:
                setattr(self, key, metadata[key])
        except:
            print('Unable to load the saved sample at ' + dataPath + '. Please make sure the data exists.')
            self = {
                'readPath': '',
                'kmerPath': ''
            }
        if ('readPath' in self.__dict__.keys()) and self.readPath:
            self.reads = spark.read.parquet(self.readPath)
        if ('kmerPath' in self.__dict__.keys()) and self.kmerPath:
            self.kmers = spark.read.parquet(self.kmerPath) 
               
    def info(self):
        # print out sample info
        print("Sample ID:%d" % self.sampleId) 
        print("Sample Name:%s" % self.sampleName)
        print("Sample dataPath:%s" % self.dataPath)
        if ('reads' in self.__dict__.keys()) and self.reads:
            print("Sample readCount:%d" % self.readCount)
        else:
            print("Sample doesn't contain any reads.")

        if ('kmers' in self.__dict__.keys()) and self.kmers:
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
            files += get_cloud_filelist(input_seq, max_files=max_files)
        if read2:
            files2 = []               
            for input_seq in read2:
                files2 += get_cloud_filelist(input_seq, max_files=max_files)
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
            reads = reads.withColumn('sid', F.lit(self.sampleId))
                
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

    def save(self, dataPath=None, key='reads', overwrite=True):
        # save a sample
        spark = SparkSession.getActiveSession()
        if dataPath: # default to sample's datapath
            self.dataPath = dataPath
        else:
            dataPath = self.dataPath

        metadata = vars(self)        

        if ('reads' in self.__dict__.keys()) and (self.reads is not None) and (key == 'reads'): #save the read data
            readPath = dataPath + 'reads.pq'
            if overwrite:
                self.reads.write.mode("overwrite").parquet(readPath)
            else:
                self.reads.write.parquet(readPath)
            metadata['readPath'] = readPath
        if ('kmers' in self.__dict__.keys()) and (self.kmers is not None) and (key == 'kmers'): #save the read data
            kmerPath = dataPath + 'kmers.pq'
            if overwrite:
                self.kmers.write.mode("overwrite").parquet(kmerPath)
            else:
                self.kmers.write.parquet(kmerPath)
            metadata['kmerPath'] = kmerPath
        # save metafile without the dataframes
        metadata.pop('reads', None)
        metadata.pop('kmers', None)
        spark.createDataFrame([metadata], schema=SAMPLE_SCHEMA).coalesce(1).write.mode("overwrite").json(dataPath)