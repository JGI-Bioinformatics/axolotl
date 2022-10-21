"""
TNF related functions

# by zhong wang @lbl.gov

This module provides:
1) calculate TNF

TODO:
Not efficient, use a JAVA version

"""

from itertools import product
import pandas as pd

reverse_complement =  ''.maketrans({'A':'T', 'T':'A', 'G':'C', 'C':'G'})
def tnf_dict():
    # return tnf dictionary, key is 4-mer, value is vector index
    alphabets = [['A', 'C', 'G', 'T'], ['A', 'C', 'G', 'T'], ['A', 'C', 'G', 'T'], ['A', 'C', 'G', 'T']]
    # all possible 4-mers (256)
    result = [''.join(p) for p in list(product(*alphabets))]
    # cononical 4-mers (136)
    tetra = {}
    counter = 0
    for r in result:
        rc = r.translate(reverse_complement)[::-1]
        if r <= rc:
            tetra[r] = counter
            counter +=1           
    return tetra

tetra = tnf_dict()
def tnf(seq, tetra=tetra):
    # use last element for non-alphabets
    tnf_vector = [0]*137
    seq = seq.upper()
    # initialize the queues
    num_kmers = len(seq)-3
    for i in range(0, num_kmers):
      tnf_vector[tetra.get(min(seq[i:i+4], seq[i:i+4].translate(reverse_complement)[::-1]), 136)] +=1
    return [t/num_kmers for t in tnf_vector[0:136]]

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



