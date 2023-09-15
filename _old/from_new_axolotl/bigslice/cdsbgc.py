from axolotl.core import AxlDF
import pyspark.sql.types as T
from pyspark.sql import Row


class cdsbgcDF(AxlDF):
    @classmethod
    def getSchema(cls) -> T.StructType:
        return T.StructType([
            T.StructField("bgc_id", T.LongType()),
            T.StructField("cds_id", T.LongType())
        ])

    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True