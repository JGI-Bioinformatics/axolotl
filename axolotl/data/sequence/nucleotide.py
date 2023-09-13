import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

from axolotl.data.sequence.base import SeqDF

from typing import Dict


class NuclSeqDF(SeqDF):
    """
    AxlDF class to store nucleotide sequences without quality values (e.g., contig sequences).
    """
    
    @classmethod
    def _getAllowedLetters(cls) -> str:
        return "ATGCNatgcn"

    @classmethod
    def _validateRowSpecific(cls, row: Row) -> bool:
        return True

    @classmethod
    def fetch_seq(cls, seq: str, loc: Dict):
        """
        a mappable class method to slice a sequence according to specific location.
        Input is a dictionary containing {"start": int, "end": int and "strand": [-1, 0, 1]}
        """
        reverse = loc.strand == -1
        if reverse:
            convert = {
            "A": "T", "T": "A", "G": "C", "C": "G",
            "a": "t", "t": "a", "g": "c", "c": "g"
            }
            snippet = seq[loc.end-1:loc.start-2:-1] if loc.start > 1 else seq[loc.end-1::-1]
            snippet = "".join([convert.get(c, c) for c in snippet])
        else:
            snippet = seq[loc.start-1:loc.end]
        return snippet


class ReadSeqDF(NuclSeqDF):
    """
    AxlDF class to store nucleotide sequences with quality values (i.e., FASTQ reads).
    This class will add an additional column, 'quality', stored as an array of
    bytes (-127 to 127)
    """
    
    @classmethod
    def _getSchemaSpecific(cls) -> T.StructType:
        return NuclSeqDF._getSchemaSpecific()\
            .add(T.StructField("quality", T.ArrayType(T.ByteType())))

    @classmethod
    def _validateRowSpecific(cls, row: Row) -> bool:
        return (
            len(row["quality"]) == row["length"]
        )


class PReadSeqDF(ReadSeqDF):
    """
    AxlDF class to store paired reads sequences. The second sequence will be stored
    in an extra column, sequence_2, along with corresponding length_2 and quality_2
    columns.
    """
    
    @classmethod
    def _getSchemaSpecific(cls) -> T.StructType:
        return ReadSeqDF._getSchemaSpecific()\
            .add(T.StructField("sequence_2", T.StringType()))\
            .add(T.StructField("length_2", T.LongType()))\
            .add(T.StructField("quality_2", T.ArrayType(T.ByteType())))

    @classmethod
    def _validateRowSpecific(cls, row: Row) -> bool:
        allowed_letters = cls._getAllowedLetters()
        return (
            ReadSeqDF._validateRowSpecific(row) and
            all(c in allowed_letters for c in row["sequence_2"]) and
            len(row["sequence_2"]) == row["length_2"] and
            len(row["quality_2"]) == row["length_2"]
        )