"""axolotl.io.csv

Contain classes definition for loading CSV files (or any custom delimiters)
"""

from axolotl.core import FlexIO
from typing import Dict


class csvIO(FlexIO):
    @classmethod
    def _getRecordDelimiter(clsI) -> str:
        return "\n"

    @classmethod
    def _parseRecord(clsI, text:str, params:Dict={}) -> Dict:
        _delim = params.get("delimiter", ",")
        cols = text.split(_delim)
        return [cols]