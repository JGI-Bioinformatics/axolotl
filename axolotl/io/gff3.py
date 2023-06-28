"""axolotl.io.gff3

Contain classes definition for loading gff3 annotation data
"""

from axolotl.core import AxlIO
from axolotl.data.feature import FeatDF
from typing import Dict


class gff3IO(AxlIO):
        
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "##gff-version 3\n"
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        res = []
        for line in text.split("\n"):
            if not line.startswith("#"):
                cols = line.split("\t")
                quals = {}
                if len(cols) == 9:
                    for pair in cols[8].split(";"):
                        key, val = pair.split("=")
                        if key not in quals:
                            quals[key] = []
                        quals[key].extend(val.split(","))
                    res.append({
                        "seq_id": cols[0],
                        "type": cols[2],
                        "location": [
                            {
                                "start": int(cols[3]),
                                "start_partial": False,
                                "end": int(cols[4]),
                                "end_partial": False,
                                "strand": 1 if cols[6] == "+" else (-1 if cols[6] == "-" else 0)
                            }
                        ],
                        "qualifiers": [
                            {
                                "key": key,
                                "values": vals
                            } for key, vals in quals.items()
                        ]
                    })
        return res

    @classmethod
    def _getOutputDFclass(cls) -> FeatDF:
        return FeatDF