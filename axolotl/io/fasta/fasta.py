import pyspark.sql.functions as F
import pyspark.sql.types as T

from axolotl.io import AxlIO
from axolotl.data.sequence.base import SeqDF
from axolotl.data.sequence import NuclSeqDF, ProtSeqDF

from abc import abstractmethod
from typing import Dict


class FastaIO(AxlIO):
    """
    AxlIO subclass to handle FASTA files (.fasta, .fa, .faa, .fna, etc.). Will return
    either ProtSeqDF or NuclSeqDF depending on the "seq_type" parameter.
    """
    
    @classmethod
    def _getOutputDFclass(cls, seq_type: str) -> SeqDF:
        if seq_type == "prot":
            return ProtSeqDF
        elif seq_type == "nucl":
            return NuclSeqDF
        else:
            raise TypeError("Unknown seq_type, please choose 'prot' or 'nucl'")
    
    @classmethod
    def _getRecordDelimiter(cls, seq_type: str) -> str:
        return "\n>"
    
    @classmethod
    def _parseRecord(cls, text:str, seq_type: str) -> Dict:
        if text[0] == ">":
            text = text[1:]
        rows = text.rstrip("\n").split("\n")
        try:
            return [{
                "seq_id": rows[0].split(" ", 1)[0],
                "desc": rows[0].split(" ", 1)[1] if " " in rows[0] else "",
                "sequence": "".join(rows[1:]),
                "length": sum(map(len, rows[1:]))
            }]
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return [None]