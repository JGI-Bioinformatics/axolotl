# Other common functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

from collections import Counter, deque
import operator, math
from itertools import product
import pandas as pd
import numpy as np

params = {
  'aws_bucket_name': None,
  'aws_key_name': None,
  'input_seq': None,
  'long_seq': None,
  'short_seq': None,
  'output_prefix': None,
  'aws_access_key': "zw-virginia-k",
  'aws_secret_key': "zw-virginia-s",
  'DEBUG': False,
  'numPartitions': 200,
  'maxRecordsPerFile': 100000,
  'key_file': '',
  'max_samples': 0,
  'sample_selected': None,
  'max_reads': 0,
  'minimizer': True,
  'k': 31,
  'n': 0,
  'min_kmer_count': 2,
  'max_kmer_count': 200,
  'min_edge_weight': 2,
  'min_reads_per_cluster': 2,
  'maxIter': 5,
  'rkmers': 10,
  'min_similarity': 0.99  
}


# faster cosine similarity according to https://gist.github.com/mckelvin/5bfad28ceb3a484dfd2a
def dot_product2(v1, v2):
    return sum(map(operator.mul, v1, v2))

def cosinesimilarity2(v1, v2):
    prod = dot_product2(v1, v2)
    len1 = math.sqrt(dot_product2(v1, v1))
    len2 = math.sqrt(dot_product2(v2, v2))
    return prod / (len1 * len2)

def most_freq_ref(x):
    """
    map reads to reference, minimumly sharing 5 kmers
    """
    m = Counter(x).most_common(1)[0]
    if m[1] < 5:
        return -1
    else:
        return m[0]
    
def most_common_percentage(x) -> FloatType():
    """
    calculate the percentage of most common elements, ignoring -1
    """
    x = [e for e in x if int(e)>-1]
    if len(x) == 0:
        return 0.0
    else:
        return Counter(x).most_common(1)[0][1]/len(x)  
def print_common(x) -> StringType():
    """
    calculate the percentage of most common elements, ignoring -1
    """
    x = [e for e in x if int(e)>-1]
    if len(x) == 0:
        return 'na'
    else:
        common = ''
        for c in Counter(x).most_common(3):
            common += ':'.join([str(e) for e in c]) + ','
        return common      
 

def save_parquet(df, output_prefix, output_suffix, sort_col='', overwrite=True):
    """
    save dataframe as parquet, by default sorted to reduce file size and optimize loading
    
    """
    if sort_col and overwrite:
        df.sort(sort_col).write.mode("overwrite").parquet(output_prefix + output_suffix)
        return
    if overwrite:
        df.write.mode("overwrite").parquet(output_prefix + output_suffix)
        return  
    df.parquet(output_prefix + output_suffix)  


def generate_kmers(seq, seqrc, k=31):
    # generate mkers given a sequence and its reverse complement
    # use a queue type of implementation to speed up
    kmers = []

    # initialize the queues
    queue = deque(seq[0:k]) 
    rcqueue = deque(seqrc[0:k]) 
    kmers.append(min([queue, rcqueue]))
    for i in range(1, len(seq)-k+1):
        queue.popleft()
        queue.append(seq[i:i+1])
        rcqueue.popleft()
        rcqueue.append(seqrc[i:i+1])        
        kmers.append(min([queue, rcqueue]))
    kmers = [k for k in kmers if 'N' not in k] # a kmer containing 'N' is filtered out
    return kmers

complement = {'A':'T', 'C':'G', 'G':'C', 'T':'A'}
def reverse_complement(seq):
  return "".join(complement.get(base, base) for base in reversed(seq))

def tnf_dict():
    # return tnf dictionary, key is 4-mer, value is vector index
    alphabets = [['A', 'C', 'G', 'T'], ['A', 'C', 'G', 'T'], ['A', 'C', 'G', 'T'], ['A', 'C', 'G', 'T']]
    # all possible 4-mers (256)
    result = [''.join(p) for p in list(product(*alphabets))]
    # cononical 4-mers (136)
    tetra = {}
    counter = 0
    for r in result:
        rc = reverse_complement(r)
        if r <= rc:
            tetra[r] = counter
            tetra[rc] = counter
            counter +=1
    return tetra

tetra = tnf_dict()

def tnf(sequence, tetra=tetra):
    # calculate TNF vector
    rc = reverse_complement(sequence)
    tnf = [0.0] * 136
    for i in range(len(sequence)-3):
        mer = sequence[i:i+4]
        rmer = rc[i:i+4]
        if mer > rmer:
          mer = rmer
        if mer in tetra:
          tnf[tetra[mer]]+=1
    length =  len(sequence)-3.0
    tnf = [i/length for i in tnf]
    return tnf

def reads_tnf(reads, method='median', sample=0):
  """
  Given a vector a reads, return tnf of a subset of the reads
  take the mean or median of tnf as the cluster tnf
  """
  # sort the reads by length, only use long ones
  if sample>0:
    reads = sorted(reads, key=len, reverse=True)[0:sample]
  reads_tnf = [tnf(r) for r in reads]
  reads_tnf = pd.DataFrame(reads_tnf)
  if method == 'median':
    return list(reads_tnf.median(axis=0)) # convert numpy object to list for spark to handle
  else:
    return list(reads_tnf.mean(axis=0)) # convert numpy object to list for spark to handle

