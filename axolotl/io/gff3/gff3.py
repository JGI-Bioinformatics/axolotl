import pyspark.sql.functions as F
import pyspark.sql.types as T

from axolotl.io import AxlIO
from axolotl.data.annotation import RawFeatDF

from typing import Dict, Callable


class gff3IO(AxlIO):
    """
    AxlIO subclass to handle GFF version 3 annotation files (.gff) and store it as RawFeatDF.
    Use fasta_path_func to supply a function that takes a GFF file path, then return the
    corresponding FASTA file path linked to that GFF. Due to the GenBank / BioPython orientated
    nature of the RawFeatDF schema, this parser will store the second column of the GFF (GFF3's
    "source") as an extra qualifier, "gff3-source-column" instead.
    """
    
    @classmethod
    def _getOutputDFclass(cls, fasta_path_func: Callable[[str], str] = None):
        return RawFeatDF
    
    @classmethod
    def _getRecordDelimiter(cls, fasta_path_func: Callable[[str], str] = None) -> str:
        return "##gff-version 3\n"
    
    @classmethod
    def _parseRecord(cls, text: str, fasta_path_func: Callable[[str], str] = None) -> Dict:
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
                        ] + [{ "key": "gff3-source-column", "values": [cols[1]] }]
                    })
        return res
    
    @classmethod
    def _postprocess(cls, data, fasta_path_func: Callable[[str], str] = None):
        """
        use the provided source_path_func to calculate the correct FASTA file path
        for each GFF file
        """

        if fasta_path_func is None:
            # change the extension into .fna
            def fasta_path_func(gff_path):
                return gff_path[::-1].replace(".gff"[::-1], ".fna"[::-1], 1)[::-1]

        data.df = data.df.withColumn("source_path", F.udf(fasta_path_func, T.StringType())("file_path"))
        
        return data