import pyspark.sql.functions as F
import pyspark.sql.types as T

from axolotl.io import AxlIO
from axolotl.data import MetaDF
from axolotl.data.sequence import NuclSeqDF
from axolotl.data.annotation import RawFeatDF

from abc import abstractmethod
from typing import Dict
from io import StringIO
import json
from Bio import SeqIO


class gbkIO(AxlIO):
    """
    AxlIO subclass to handle GenBank files (.gbk, .gb). Will either extract the nucleotide
    sequences, annotation features or contig/chromosome's metadata according to the specified
    'df_type' parameter.
    """
    
    @classmethod
    def _getOutputDFclass(cls, df_type: str):
        if df_type == "sequence":
            return NuclSeqDF
        elif df_type == "annotation":
            return RawFeatDF
        elif df_type == "metadata":
            return MetaDF
        else:
            raise TypeError("Unknown df_type, please choose 'metadata', 'sequence' or 'annotation'")
    
    @classmethod
    def _getRecordDelimiter(cls, df_type: str) -> str:
        return "\n//\n"
    
    @classmethod
    def _parseRecord(cls, text: str, df_type: str) -> Dict:
        if text[0] == "//":
            text = text[1:]
        text = text.rstrip("\n")
        try:
            if df_type == "sequence":
                with StringIO(text + "\n//\n") as stream:
                    for record in SeqIO.parse(stream, "genbank"):
                        # should only return one record
                        return [{
                            "seq_id": record.id,
                            "desc": record.description,
                            "sequence": str(record.seq),
                            "length": len(record.seq)
                        }]
            elif df_type == "annotation":
                res = []
                with StringIO(text + "\n//\n") as stream:
                    for record in SeqIO.parse(stream, "genbank"):
                        for feature in record.features:
                            res.append({
                                "seq_id": record.id,
                                "type": feature.type,
                                "location": [
                                    {
                                        "start": int(feature.location.start),
                                        "start_partial": False,
                                        "end": int(feature.location.end),
                                        "end_partial": False,
                                        "strand": int(feature.location.strand)
                                }],
                                "qualifiers": [
                                    {
                                        "key": key,
                                        "values": qual
                                    } for key, qual in feature.qualifiers.items()
                                ]
                            })
                return res
            elif df_type == "metadata":
                with StringIO(text + "\n//\n") as stream:
                    for record in SeqIO.parse(stream, "genbank"):
                        # should only return one record
                        return [{
                            "key": key, "value": val
                        } for key, val in {
                            **{
                                "id": record.id,
                                "name": record.name,
                                "description": record.description,
                            }, **{
                                key: (
                                    val if isinstance(val, str) else json.dumps(val)
                                ) for key, val in record.annotations.items()
                            }
                        }.items()]
            else:
                raise Exception()
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return [None]