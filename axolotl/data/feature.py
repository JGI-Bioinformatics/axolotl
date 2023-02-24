"""axolotl.data.feature

Contain classes definition for SeqFeature DataFrames.
"""

from pyspark.sql import DataFrame, Row, types
from pyspark.sql import SparkSession
from abc import abstractmethod

from axolotl.core import ioDF


class FeatDF(ioDF):
    """actual annotation dataframe based on GenBank features"""
    
    @classmethod
    def _getSchemaSpecific(cls) -> types.StructType:
        return types.StructType([
            types.StructField("seq_id", types.StringType()),
            types.StructField("type", types.StringType()),
            types.StructField("location", types.ArrayType(
                types.StructType([
                    types.StructField("start", types.LongType()),
                    types.StructField("end", types.LongType()),
                    types.StructField("start_partial", types.BooleanType()),
                    types.StructField("end_partial", types.BooleanType()),
                    types.StructField("strand", types.LongType())
                ])
            )),
            types.StructField("qualifiers", types.ArrayType(
                types.StructType([
                    types.StructField("key", types.StringType()),
                    types.StructField("values", types.ArrayType(types.StringType()))
                ])
            ))
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True