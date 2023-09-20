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
    def _getRecordDelimiter(cls, prefix: str="") -> str:
        return "\n"

    @classmethod
    @abstractmethod
    def _getOutputDFclass(cls, prefix: str="") -> MetaDF:
        return MetaDF

    @classmethod
    @abstractmethod
    def _parseRecord(cls, text: str, prefix: str="") -> Dict:
        key, value = text.rstrip("\n").split(",")
        return [{
            "key": prefix + key,
            "value": value
        }]

    @classmethod
    def _prepInput(cls, file_path: str, tmp_dir: str, prefix: str="") -> str:
        temp_file = path.join(tmp_dir, "temp.txt")
        with open(file_path, "r") as input_stream:
            with open(temp_file, "w") as output_stream:
                for line in input_stream:
                    output_stream.write("preprocessed-" + prefix + line)
        return temp_file

    @classmethod
    def _postprocess(cls, data: MetaDF, prefix: str="") -> MetaDF:
        new_df = data.df.withColumn("value", F.when(F.lit(True), F.lit("postprocessed")))
        return cls._getOutputDFclass(prefix)(new_df, keep_idx=True)