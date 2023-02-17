"""axolotl.io.fastq

Contain classes definition for loading single and paired FASTQ data.

TODO:
- implement paired reads parsing
"""

from axolotl.core import AxolotlIO
from axolotl.data.seq import ReadSeqDF
from typing import Dict, List


class FastqIO(AxolotlIO):

    # quality metrics
    QUAL_PHRED33 = {
        c: q for q, c in enumerate(
            "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        )
    }
    QUAL_PHRED64 = {
        c: q for q, c in enumerate(
            "@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefgh"
        )
    }
    QUAL_SOLEXA64 = {
        c: q-5 for q, c in enumerate(
            ";<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefgh"
        )
    }

    @classmethod
    def decodeQual(cls, seq:str, encoding:str) -> List[int]:
        encoder_list = {
            "phred+33": cls.QUAL_PHRED33,
            "phred+64": cls.QUAL_PHRED64,
            "solexa+64": cls.QUAL_SOLEXA64
        }
        return [encoder_list[encoding][x] for x in seq]
    
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
        encoding = params.get("encoding", "phred+33") 
        try:
            return {
                "seq_id": rows[0].split(" ", 1)[0],
                "desc": rows[0].split(" ", 1)[1] if " " in rows[0] else "",
                "sequence": rows[1],
                "length": len(rows[1]),
                "quality_scores": cls.decodeQual(rows[3], encoding)
            }
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return None