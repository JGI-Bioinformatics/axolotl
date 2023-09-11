from abc import abstractmethod
from axolotl.core import AxlDF
from axolotl.workflow import AxlWorkFlow
from axolotl.data.seq import NuclSeqDF
from axolotl.data.feature import FeatDF
from axolotl.data.bgc import bgcDF
from axolotl.data.cds import cdsDF
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row
from typing import Dict

from axolotl.workflow.bigslice.cdsbgc import cdsBGCDF


class BigsliceWorkFlow(AxlWorkFlow):

    @classmethod
    @abstractmethod
    def _dataDesc(cls) -> Dict:
        return {
            "bgc": bgcDF,
            "cds": cdsDF,
            "cds_to_bgc": cdsBGCDF
        }
        
    @abstractmethod
    def _load_extra_data(self):
        pass

    @abstractmethod
    def _create(self, seq_df: NuclSeqDF, feature_df: FeatDF, source_type: str="antismash"):
        self._setData("cds", cdsDF.fromFeatDF(feature_df, seq_df))
        self._saveData("cds")
        self._setData("bgc", bgcDF.fromFeatDF(feature_df, seq_df, source_type))
        self._saveData("bgc")

        bgc = self._getData("bgc")
        cds = self._getData("cds")
        self._setData("cds_to_bgc", cdsBGCDF(bgc.df.join(cds.df, [
            bgc.df.file_path == cds.df.file_path,
            bgc.df.source_path == cds.df.source_path,
            bgc.df.seq_id == cds.df.seq_id,
            bgc.df.location.start <= cds.df.location.start,
            bgc.df.location.end >= cds.df.location.end
        ]).select(
            F.when(F.lit(True), bgc.df.row_id).alias("bgc_id"),
            F.when(F.lit(True), cds.df.row_id).alias("cds_id")
        )))
        self._saveData("cds_to_bgc")