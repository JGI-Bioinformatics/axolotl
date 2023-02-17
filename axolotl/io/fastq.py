"""axolotl.io.fastq

Contain classes definition for loading single and paired FASTQ data.

TODO:
- implement paired reads parsing
"""

from axolotl.core import AxolotlIO
from axolotl.data.seq import ReadSeqDF
from typing import Dict


class FastqIO(AxolotlIO):

    # quality metrics
    QUAL_PHRED33 = {
        c: q for q, c in enumerate(
            "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        )
    }
    
    @classmethod
    def _getRecordDelimiter(cls) -> str:
        return "\n@"
    
    @classmethod
    def _getOutputDFclass(cls) -> ReadSeqDF:
        return ReadSeqDF
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        # TODO: parse quality other than Phred+33
        if text[0] == "@":
            text = text[1:]
        rows = text.rstrip("\n").split("\n")
        try:
            return {
                "seq_id": rows[0].split(" ", 1)[0],
                "desc": rows[0].split(" ", 1)[1] if " " in rows[0] else "",
                "sequence": rows[1],
                "length": len(rows[1]),
                "quality_scores": [cls.QUAL_PHRED33[x] for x in rows[3]]
            }
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return None