"""
...
"""

from axolotl.data.genotype import vcfDF
from axolotl.core import AxlSet, MetaDF

from typing import Dict


class vcfSet(AxlSet):

    @classmethod
    def _dataDesc(cls) -> Dict:
        return {
            "metadata": MetaDF,
            "data": vcfDF
        }