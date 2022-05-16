"""
TNF related functions

# by zhong wang @lbl.gov

This module provides:
1) calculate TNF

TODO:
Not efficient, use a JAVA version

"""

import pyspark.sql.functions as F
from pyspark.sql.types import *
from collections import Counter, deque
from itertools import product
import pandas as pd
import numpy as np


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
