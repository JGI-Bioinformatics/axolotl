# axolotl.bio utils

def calc_kmer(text:str, k:int, blacklist_chars:str="", as_dict:bool=False):
	"""calculate k-mers for any given string"""
    blacklist_chars = set(blacklist_chars)
    n = len(text)
    i = 0
    result = {}
    while n >= i + k:
        mer = text[i:i+k]
        result[mer] = result[mer] + 1 if mer in result else 1
        i += 1
    if len(blacklist_chars) > 0:
        result = {
            mer: count for mer, count in result.items()\
            if len(set.intersection(blacklist_chars, set(mer))) < 1
        }
    if as_dict:
    	return result
    else:
	    return [(key, val) for key, val in result.items()]