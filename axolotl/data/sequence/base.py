import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

from axolotl.data import ioDF

from abc import abstractmethod
from typing import Dict


class SeqDF(ioDF):
    """
    AxlDF Abstract subclass to handle genomic sequence data types (i.e., from a FASTA file).
    You can't instantiate this class directly. Instead, import the concrete subclasses such
    as NuclSeqDF, ContigDF or ProtSeqDF.

    Example DataFrame content (apply for all subclasses):

    -------------------------------------------------------------------------------------------------------------
    | idx | file_path | seq_id   | desc       | sequence        | length    | ..<subclass-specific columns>...  |
    -------------------------------------------------------------------------------------------------------------
    | 1   | /test.fa  | ctg_001  | contig_1   | ATGCATGCATGC... | 120       | ...                               |
    | 2   | /test.fa  | ctg_002  | contig_2   | ATGCATGCATGC... | 120       | ...                               |
    -------------------------------------------------------------------------------------------------------------
    """
    
    @classmethod
    def _getSchemaSpecific(cls) -> T.StructType:
        """
        generally, subclasses will not need to append their own columns. However, when additional columns are needed,
        make sure to explicitly include this class's schema in the subclass's _getSchemaSpecific() method.

        e.g.,:

        @classmethod
        def _getSchemaSpecific(cls) -> T.StructType:
            return SeqDF._getSchemaSpecific()\
                .add(T.StructField("new_column_1", T.StringType()))\
                .add(T.StructField("new_column_2", T.StringType()))\

        """
        return T.StructType([
            T.StructField('seq_id', T.StringType()),
            T.StructField('desc', T.StringType()),
            T.StructField('sequence', T.StringType()),
            T.StructField('length', T.LongType())
        ])

    @classmethod
    def validateRow(cls, row: Row) -> bool:
        """
        by default, this class support specifying the allowed letters (in a string of characters)
        of the sequence. Any subclass can then implement its own extra validation via _validateRowSpecific()
        """
        allowed_letters = cls._getAllowedLetters()
        return (
            all(c in allowed_letters for c in row["sequence"]) and
            len(row["sequence"]) == row["length"] and
            row["length"] > 0 and
            cls._validateRowSpecific(row)
        )

    ##### TO BE IMPLEMENTED BY SUBCLASSES #####

    @classmethod
    @abstractmethod
    def _getAllowedLetters(cls) -> str:
        raise NotImplementedError("calling an unimplemented abstract method _getAllowedLetters()")
        
    @classmethod
    @abstractmethod
    def _validateRowSpecific(cls, row: Row) -> bool:
        raise NotImplementedError("calling an unimplemented abstract method _validateRowSpecific()")