from abc import ABC, abstractmethod
from os import path
from typing import Dict

import pyspark.sql.functions as F
import pyspark.sql.types as T

from axolotl.io.base import AxlIO
from axolotl.data import MetaDF


class DummyIO(AxlIO):

    @classmethod
    @abstractmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n"

    @classmethod
    @abstractmethod
    def _getOutputDFclass(cls) -> MetaDF:
        return MetaDF

    @classmethod
    @abstractmethod
    def _parseRecord(cls, text: str, params:Dict={}) -> Dict:
        key, value = text.rstrip("\n").split(",")
        return [{
            "key": key,
            "value": value
        }]

    @classmethod
    def _prepInput(cls, file_path: str, tmp_dir: str) -> str:
        temp_file = path.join(tmp_dir, "temp.txt")
        with open(file_path, "r") as input_stream:
            with open(temp_file, "w") as output_stream:
                for line in input_stream:
                    output_stream.write("preprocessed-" + line)
        return temp_file

    @classmethod
    def _postprocess(cls, data: MetaDF, params: Dict={}) -> MetaDF:
        new_df = data.df.withColumn("value", F.lit("postprocessed"))
        return cls._getOutputDFclass()(new_df, keep_idx=True)