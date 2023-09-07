"""
set containing bgc, cds and cds_to_bgc dataframes
"""

from axolotl.core import AxlDF
from axolotl.data.cds import cdsDF
from axolotl.data.bgc import bgcDF
from axolotl.core import AxlSet, MetaDF
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row
from typing import Dict


class bgcSet(AxlSet):

    class __cdsbgcDF(AxlDF):
        @classmethod
        def getSchema(cls) -> T.StructType:
            return T.StructType([
                T.StructField("bgc_id", T.LongType()),
                T.StructField("cds_id", T.LongType())
            ])

        @classmethod
        def validateRow(cls, row: Row) -> bool:
            return True

    @classmethod
    def _dataDesc(cls) -> Dict:
        return {
            "cds": cdsDF,
            "bgc": bgcDF,
            "cds_to_bgc": cls.__cdsbgcDF
        }

    @classmethod
    def create(cls, cds: cdsDF, bgc: bgcDF):

        cds_bgc_df = bgc.df.join(cds.df, [
            bgc.df.file_path == cds.df.file_path,
            bgc.df.source_path == cds.df.source_path,
            bgc.df.seq_id == cds.df.seq_id,
            bgc.df.location.start <= cds.df.location.start,
            bgc.df.location.end >= cds.df.location.end
        ]).select(
            F.when(F.lit(True), bgc.df.row_id).alias("bgc_id"),
            F.when(F.lit(True), cds.df.row_id).alias("cds_id")
        )
        
        return cls({
            "cds": cdsDF(cds_bgc_df.join(cds.df.alias("cds"), [
                cds_bgc_df.cds_id == cds.df.row_id
            ]).select("cds.*")),
            "bgc": bgc,
            "cds_to_bgc": cls.__cdsbgcDF(cds_bgc_df)
        })