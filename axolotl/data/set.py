"""axolotl.data.set

Contain classes definition for record-type (multi-dataframes dataset) data
"""

from axolotl.data.feature import FeatDF
from axolotl.data.seq import NuclSeqDF
from axolotl.data.genotype import vcfDF
from axolotl.core import AxlSet, MetaDF

from typing import Dict


class gbkSet(AxlSet):

    @classmethod
    def _dataDesc(cls) -> Dict:
        return {
            "sequence": NuclSeqDF,
            "annotation": FeatDF
        }

    @classmethod
    def create(cls, sequence: NuclSeqDF, annotation: FeatDF):
        return cls({
            "sequence": sequence,
            "annotation": annotation
        })



class vcfSet(AxlSet):

    @classmethod
    def _dataDesc(cls) -> Dict:
        return {
            "metadata": MetaDF,
            "data": vcfDF
        }