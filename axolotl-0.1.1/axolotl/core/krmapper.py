"""

# functions related to k-mer analyses
# Take a reads dataframe, generate kmer/minizer to read mappings

# by Zhong Wang @ lbl.gov

# test kmer mapping/filter function:

# java udf kmer function
spark = SparkSession.builder.getOrCreate()
spark.udf.registerJavaFunction("jkmerudf", "org.jgi.spark.udf.jkmer", ArrayType(LongType()))

from axolotl.core.krmapper import *
from axolotl.io.samples import *

input_seq = 'dbfs:/mnt/gis20mock/gis20mock/pacbio.seq'
datapath = 'dbfs:/mnt/gis20mock/gis20mock/pacbio_'
sample1 = Sample(sampleId=0, sampleName='pacbio_test')
sample1.import_sequences(read1=[input_seq], read2=[], format='seq', max_files=0, max_reads=1000, remove_non_alphabet=False)
sample1.save(datapath=datapath, overwrite=False) 

k = 31
minimizer = True
m = int(k/2) + 1 if minimizer else k
n = 0
min_kmer_count = 2

kmers = reads_to_kmersu(sample1.reads, k=k, m=m, n=n).select('id', 'sid', 'kmerlist').cache()
if not hasattr(sample1, 'kmers'):
    setattr(sample1, 'kmers', None)

sample1.kmers = kmer_filter(kmers, min_kmer_count=min_kmer_count)
print('{:,d}'.format(sample1.kmers.count()) + " kmers/mmers passed filter.")   

#save kmers
sample1.save(key='kmers')

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

from collections import Counter, deque


"""
IUPAC codes for ambigious bases
M	A or C	K
R	A or G	Y
W	A or T	W
S	C or G	S
Y	C or T	R
K	G or T	M
V	A or C or G	B
H	A or C or T	D
D	A or G or T	H
B	C or G or T	V
N
"""
# use two bit to pack sequence
base2bit = {'A':'00', 'T':'11', 'C':'01', 'G':'10', 
            'N':'22', 'Y':'22', 'M':'22', 'R':'22', 
            'W':'22', 'S':'22', 'K':'22', 'V':'22', 
            'H':'22', 'D':'22', 'B':'22', 'a':'00',
            't':'11', 'c':'01', 'g':'10', 'n':'22'
           }

def generate_mmers_2bit(seq, seqrc, k=31, m=16, n=0):
    # generate mmers given two int vectors
    # added an option to return a subset of mimizers
    mmers = []
    for i in range(0, len(seq)-k+1):
        kmer = seq[i:(i+k)]
        kmerc = seqrc[i:(i+k)]
        mmers.append( min([''.join(kmer[j:(j+m)]) for j in range(0, k-m+1)] + [''.join(kmerc[j:(j+m)]) for j in range(0, k-m+1)]))
    mmers = [int(m, 2) for m in set(mmers) if '2' not in m] # minimizers containing 'N' is filtered out
    if n < 1:
        return mmers
    else:
        return sorted(mmers)[0:n]

def generate_mmers_2bit2(seq, seqrc, k=31, m=16, n=0):
    # generate mmers given two int vectors
    # added an option to return a subset of mimizers
    # use a queue type of implementation to speed up
    mmers = []

    # initialize the queues
    queue = deque([''.join(seq[0:k][j:(j+m)]) for j in range(0, k-m+1)]) 
    rcqueue = deque([''.join(seqrc[0:k][j:(j+m)]) for j in range(0, k-m+1)])
    mmers.append(min(queue + rcqueue))
    for i in range(1, len(seq)-k+1):
        queue.popleft()
        queue.append(''.join(seq[i+k-m:i+k]))
        rcqueue.popleft()
        rcqueue.append(''.join(seqrc[i+k-m:i+k]))        
        mmers.append(min(queue + rcqueue))
    mmers = [int(m, 2) for m in set(mmers) if '2' not in m] # minimizers containing 'N' is filtered out
    if n < 1:
        return mmers
    else:
        return sorted(mmers)[0:n]

def find_rkmers(all_kmers, n=100):
    # find rkmers
    all_kmers = Counter(all_kmers).most_common()
    all_kmers = [kmer for kmer, count in all_kmers if count>1]
    num_kmers = len(all_kmers)
    if num_kmers <= n:
        return all_kmers
    else:
        start = int((num_kmers-n)/2)
        return all_kmers[start:start+n]  

 
def reads_to_kmers(reads_df, k=31, m=15, n=0):
    """
    given a reads dataframe with columns ['id', 'sid', 'seq' ...]
    calculate kmer to ids mapping
    """
    # kmer dataframe: kmer(Integer), kmer_count(Integer). Ids(Array(Integer))
    base2t = F.udf(lambda x: [base2bit[i] for i in x], ArrayType(StringType()))
    generate_mmer_udf = F.udf(lambda arr: generate_mmers_2bit(arr[0], arr[1], k=k, m=m, n=n), ArrayType(LongType()))
   
    return (reads_df
            .withColumn('rc', F.translate(F.reverse('seq'), 'AGCT', 'TCGA')) # reverse-complement the sequence
            .withColumn('2bit', base2t('seq')) # this change letters to 2bits of forward seq
            .withColumn('2bitrc', base2t('rc'))  # this change letters to 2bits of reverse seq
            .withColumn('kmerlist', generate_mmer_udf(F.array('2bit', '2bitrc'))) # generate mimizers
           )

def reads_to_kmers2(reads_df, k=31, m=15, n=0):
    """
    given a reads dataframe with columns ['id', 'sid', 'seq' ...]
    calculate kmer to ids mapping
    """
    # kmer dataframe: kmer(Integer), kmer_count(Integer). Ids(Array(Integer))
    base2t = F.udf(lambda x: [base2bit[i] for i in x], ArrayType(StringType()))
    generate_mmer_udf = F.udf(lambda arr: generate_mmers_2bit2(arr[0], arr[1], k=k, m=m, n=n), ArrayType(LongType()))
   
    return (reads_df
            .withColumn('rc', F.translate(F.reverse('seq'), 'AGCT', 'TCGA')) # reverse-complement the sequence
            .withColumn('2bit', base2t('seq')) # this change letters to 2bits of forward seq
            .withColumn('2bitrc', base2t('rc'))  # this change letters to 2bits of reverse seq
            .withColumn('kmerlist', generate_mmer_udf(F.array('2bit', '2bitrc'))) # generate mimizers
           )           

def reads_to_kmersu(reads_df, k=31, m=15, n=0):
    """
    given a reads dataframe with columns ['id', 'sid', 'seq' ...]
    calculate kmer to ids mapping
    use java UDF
    w = k-m
    """
    spark = SparkSession.getActiveSession()
    # kmer dataframe: kmer(Integer), kmer_count(Integer). Ids(Array(Integer))
    reads_df.createOrReplaceTempView("reads")
    w = k - m
    if w <= 0:
        w = 1 # kmers
    query = 'SELECT id, sid, seq, jkmerudf(seq, {}, {}) as kmerlist from reads'.format(m,w)
    return spark.sql(query)

def kmer_filter(reads_df, min_kmer_count=2):
    """
    filter out erroreous kmers, input dataframe has ('id', 'sid', 'kmerlist')
    output dataframe has ('id', 'sid', 'kmer', 'kmer_count', 'Ids')
    """
    return (reads_df
            .select('id', 'sid', F.explode("kmerlist").alias('kmer'))
            .groupby('kmer')
            .agg(F.count(F.lit(1)).alias("kmer_count"), F.collect_list('id').alias('Ids'))
            .where(F.col("kmer_count") >=min_kmer_count)
           )
