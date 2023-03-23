"""axolotl.io.vcf

Contain classes definition for loading VCF genotype files

"""

from axolotl.core import AxolotlRecord, AxolotlIO, recordIO, MetaDF, ioDF
from axolotl.utils.file import fopen
from typing import List, Dict, Tuple
from os import path

from axolotl.data.genotype import vcfDF
from axolotl.data.record import vcfRecord


class vcfMetaIO(AxolotlIO):
        
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n#" # capture the first '#', then later check what it's followed with
    
    @classmethod
    def _getOutputDFclass(cls) -> MetaDF:
        return MetaDF
    
    @classmethod
    def loadSmallFiles(cls, file_pattern:str, minPartitions:int=None, params:Dict={}) -> ioDF:
        raise Exception("Please use loadBigFiles() for this type of files")

    @classmethod
    def loadConcatenatedFiles(cls, file_pattern:str, minPartitions:int=None, persist:bool=True, intermediate_pq_path:str="", params:Dict={}) -> ioDF:
        raise Exception("Please use loadBigFiles() for this type of files")
        
    @classmethod
    def concatSmallFiles(cls, file_pattern:str, path_output:str, num_partitions:int=-1):
        raise Exception("Please use loadBigFiles() for this type of files")
    
    @classmethod
    def _prepInput(cls, file_path:str, tmp_dir:str) -> str:
        
        temp_file = path.join(tmp_dir, "temp.vcf")
        
        with fopen(file_path, "r") as ii:
            with fopen(temp_file, "w") as oo:
                for line in ii:
                    if line.startswith("##"):
                        oo.write(line)
                    elif line.startswith("#CHROM"):
                        break
        
        return temp_file
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        if text[0:2] == "##":
            text = text[1:]
        elif not text[0] == "#":
            return [] # not a metadata row
        text = text[1:].rstrip("\n")
        try:
            return [{
                "key": text.split("=", 1)[0],
                "value": text.split("=", 1)[1]
            }]
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return [None]
        
        
class vcfDataIO(AxolotlIO):
    
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n"
    
    @classmethod
    def _getOutputDFclass(cls) -> vcfDF:
        return vcfDF
        
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        if text.startswith("#"):
            return [] # skip
        else:
            cols = text.rstrip("\n").split("\t")
            cols = cols[:8] + [cols[8:] if len(cols) > 8 else []]
            return [cols]
        
        
class vcfIO(recordIO):
        
    @classmethod
    def _getOutputIOclasses(cls) -> Dict:
        return {
            "metadata": vcfMetaIO,
            "data": vcfDataIO
        }
    
    @classmethod
    def _getOutputDFclass(cls) -> AxolotlRecord:
        return vcfRecord