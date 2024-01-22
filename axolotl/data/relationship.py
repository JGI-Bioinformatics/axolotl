import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

from axolotl.data import AxlDF


class RelationDF(AxlDF):
    """
    AxlDF subclass to handle 'relationship' between two Axolotl DataFrames.
    idx_1 for table #1, idx_2 for table #2.

    Example DataFrame content:

    --------------------------------
    | idx | idx_1 | idx_2          |
    --------------------------------
    | 1   | 1     | 1              |
    | 2   | 1     | 2              |
    --------------------------------
    """
        
    @classmethod
    def _getSchema(cls) -> T.StructType:
        return T.StructType([
            T.StructField("idx_1", T.LongType()),
            T.StructField("idx_2", T.LongType())
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return (
            row["idx_1"] is not None and\
            row["idx_2"] is not None
        )