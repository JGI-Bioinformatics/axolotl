"""axolotl.io.gff3

Contain classes definition for loading gff3 annotation data
"""

from axolotl.core import AxlIO
from axolotl.data.feature import FeatDF
from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

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
                        key, val = pair.split("=") if "=" in pair else [pair, ""]
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

    @classmethod
    def _postprocess(cls, data:ioDF, params:Dict={}) -> Dict:
        if "source_path_func" in params:
            processing_func = params["source_path_func"]
        else:
            def processing_func(gff_path):
                return gff_path[::-1].replace(".gff"[::-1], ".fna"[::-1], 1)[::-1]
        data.df = data.df.withColumn("source_path", F.udf(processing_func, StringType())("file_path"))
        return data