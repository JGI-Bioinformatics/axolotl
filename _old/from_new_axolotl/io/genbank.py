"""axolotl.io.genbank

Contain classes definition for loading genbank annotation and sequence data

TODO:
- parse contigs metadata
- add more interactivity (more like from the base AxlSet object)
"""

from axolotl.core import setIO, AxlIO
from axolotl.core import AxlSet
from axolotl.data.set import gbkSet
from axolotl.data.feature import FeatDF
from axolotl.data.seq import NuclSeqDF

from Bio import SeqIO
from io import StringIO

from typing import Dict


class gbkSeqIO(AxlIO):
        
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n//\n"
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        if text[0] == "//":
            text = text[1:]
        text = text.rstrip("\n")
        try:
            with StringIO(text + "\n//\n") as stream:
                return [{
                    "seq_id": record.id,
                    "desc": record.description,
                    "sequence": str(record.seq),
                    "length": len(record.seq)
                } for record in SeqIO.parse(stream, "genbank")]
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return [None]

    @classmethod
    def _getOutputDFclass(cls) -> NuclSeqDF:
        return NuclSeqDF


class gbkFeatIO(AxlIO):
        
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n//\n"
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        if text[0] == "//":
            text = text[1:]
        text = text.rstrip("\n")
        try:
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
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return [None]

    @classmethod
    def _getOutputDFclass(cls) -> FeatDF:
        return FeatDF

    
class gbkIO(setIO):
    
    
    @classmethod
    def _getOutputIOclasses(cls) -> Dict:
        return {
            "sequence": gbkSeqIO,
            "annotation": gbkFeatIO
        }
    
    @classmethod
    def _getOutputDFclass(cls) -> AxlSet:
        return gbkSet