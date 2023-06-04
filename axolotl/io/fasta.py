"""axolotl.io.fasta

Contain classes definition for loading proteins and nucleotides FASTA data.

TODO:
"""

from axolotl.core import AxlIO
from axolotl.data.seq import NuclSeqDF, ProtSeqDF
from typing import Dict


class FastaIO(AxlIO):
    
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n>"
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        if text[0] == ">":
            text = text[1:]
        rows = text.rstrip("\n").split("\n")
        try:
            return [{
                "seq_id": rows[0].split(" ", 1)[0],
                "desc": rows[0].split(" ", 1)[1] if " " in rows[0] else "",
                "sequence": rows[1],
                "length": len(rows[1])
            }]
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return [None]


class NuclFastaIO(FastaIO):
    
    @classmethod
    def _getOutputDFclass(cls) -> NuclSeqDF:
        return NuclSeqDF
    

class ProtFastaIO(FastaIO):
    
    @classmethod
    def _getOutputDFclass(cls) -> ProtSeqDF:
        return ProtSeqDF
    