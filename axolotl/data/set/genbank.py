"""
...
"""

from axolotl.data.feature import FeatDF
from axolotl.data.seq import NuclSeqDF
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