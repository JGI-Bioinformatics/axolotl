"""axolotl.data.record

Contain classes definition for record-type (multi-dataframes) data
"""

from axolotl.data.feature import FeatDF
from axolotl.data.seq import NuclSeqDF
from axolotl.core import AxolotlRecord

from typing import Dict

class gbkRecord(AxolotlRecord):

    @classmethod
    def _dataDesc(cls) -> Dict:
        return {
            "sequence": NuclSeqDF,
            "annotation": FeatDF
        }