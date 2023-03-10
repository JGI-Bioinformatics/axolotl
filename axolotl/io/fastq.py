"""axolotl.io.fastq

Contain classes definition for loading single and paired FASTQ data.

TODO:
- implement paired reads parsing
"""

from axolotl.core import AxolotlIO
from axolotl.data.seq import ReadSeqDF
from typing import Dict, List

from axolotl.utils.file import parse_path_type, fopen, gunzip_deflate
from os import path


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
    def _prepInput(cls, file_path:str, tmp_dir:str) -> str:
        match = parse_path_type(file_path)

        if len(list(filter(match["path"].endswith, [".fastq", ".fq"]))) > 0:
            # no preprocessing needed
            return file_path
        elif len(list(filter(match["path"].endswith, [".fastq.gz", ".fq.gz"]))) > 0:
            # unzip fastq into the temp folder
            output_path = path.join(tmp_dir, "extracted.fastq")
            gunzip_deflate(file_path, output_path)
            return output_path
        else:
            raise Exception("can't recognize file extension")

        return file_path
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        # TODO: parse quality other than Phred+33
        if text[0] == "@":
            text = text[1:]
        rows = text.rstrip("\n").split("\n")
        encoding = params.get("encoding", "phred+33") 
        try:
            return [{
                "seq_id": rows[0].split(" ", 1)[0],
                "desc": rows[0].split(" ", 1)[1] if " " in rows[0] else "",
                "sequence": rows[1],
                "length": len(rows[1]),
                "quality_scores": cls.decodeQual(rows[3], encoding)
            }]
        except:
            print("WARNING: failed parsing a malformed record text '{}'".format(
                text if len(text) < 50 else text[:50] + "..."
            ))
            return [None]