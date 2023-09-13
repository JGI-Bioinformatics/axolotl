import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

from axolotl.data.sequence.base import SeqDF

from typing import Dict


class ProtSeqDF(SeqDF):
    """
    AxlDF class to store amino acid sequences (including gaps and terminals)
    """

    @classmethod
    def _getAllowedLetters(cls) -> str:
        return "ABCDEFGHIJKLMNOPQRSTUVWYZX*-abcdefghijklmnopqrstuvwyzx"
    
    @classmethod
    def _validateRowSpecific(cls, row: Row) -> bool:
        return True