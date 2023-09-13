import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

from axolotl.data import ioDF


class MetaDF(ioDF):
    """
    AxlDF subclass to handle 'metadata' of a source raw file as key-value pairs.
    For example, a GenBank file can have 'DEFINITION', 'ACCESSION', ... as its metadata

    Example DataFrame content:

    ---------------------------------------------------
    | idx | file_path | key          | value          |
    ---------------------------------------------------
    | 1   | /test.gbk | DEFINITION   | streptomyces   |
    | 2   | /test.gbk | ACCESSION    | NC0001         |
    ---------------------------------------------------
    """
        
    @classmethod
    def _getSchemaSpecific(cls) -> T.StructType:
        return T.StructType([
            T.StructField("key", T.StringType()),
            T.StructField("value", T.StringType())
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return row["value"] is not None