"""axolotl.io.fasta

Contain classes definition for loading proteins and nucleotides FASTA data.

TODO:
"""

from axolotl.core import AxolotlIO
from axolotl.data.sequence import NuclSequenceDF, ProtSequenceDF
from typing import Dict


class FastaIO(AxolotlIO):
    
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n>"
    
    @classmethod
    def _parseRecord(cls, text:str) -> Dict:
        # TODO: parse quality other than Phred+33
        if text[0] == "@":
            text = text[1:]
        rows = text.rstrip("\n").split("\n")
        try:
            return {
                "seq_id": rows[0].split(" ", 1)[0],
                "desc": rows[0].split(" ", 1)[1] if " " in rows[0] else "",
                "sequence": rows[1],
                "length": len(rows[1])
            }
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return None


class NuclFastaIO(FastaIO):
    
    @classmethod
    def _getOutputDFclass(cls) -> NuclSequenceDF:
        return NuclSequenceDF
    

class ProtFastaIO(FastaIO):
    
    @classmethod
    def _getOutputDFclass(cls) -> ProtSequenceDF:
        return ProtSequenceDF
    