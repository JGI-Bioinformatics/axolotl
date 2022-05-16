"""
# Other common functions
# pythonic, not spark

"""


from collections import Counter
import operator, math


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


