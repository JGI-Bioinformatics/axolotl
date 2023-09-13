"""axolotl.data.genotype

Contain classes definition for handing genotype VCF and PGS data
"""

from pyspark.sql import Row, types
from axolotl.core import ioDF


class vcfDF(ioDF):
        
    @classmethod
    def _getSchemaSpecific(cls) -> types.StructType:
        return types.StructType([
            types.StructField("chromosome", types.StringType()),
            types.StructField("position", types.StringType()),
            types.StructField("ids", types.StringType()),
            types.StructField("references", types.StringType()),
            types.StructField("alts", types.StringType()),
            types.StructField("qual", types.StringType()),
            types.StructField("filter", types.StringType()),
            types.StructField("info", types.StringType()),
            types.StructField("genotypes", types.ArrayType(types.StringType()))
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True