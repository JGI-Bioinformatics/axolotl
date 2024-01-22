import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

from axolotl.data import ioDF

from typing import Dict, List


class RawFeatDF(ioDF):
    """
    AxlDF subclass to handle raw genomic annotation features (i.e., from GenBank and GFF files). This DataFrame is
    conceptually similar to the BioPython's SeqFeature structure. Importantly, all feature 'qualifiers' are stored
    in a single column as a list of key-values pairs. This class is meant to act as a raw data storage for genomic
    features, which will then be further postprocessed by other classes extracting only relevant information out of
    the DataFrame (see examples in axolotl.data.bgc and axolotl.data.cds).

    Example DataFrame content:

    ----------------------------------------------------------------------------------------------------------------------------------------
    | idx | file_path  | source_path | seq_id   | type  | location                         | qualifiers                                    |
    ----------------------------------------------------------------------------------------------------------------------------------------
    | 1   | /test.gff  | /test.fa    | contig_1 | CDS   | [{"start": 1, "end": 150, ...}]  | [{"key": "locus_tag", "values": ["cds0001"]}] |
    | 2   | /test.gff  | /test.fa    | contig_1 | CDS   | [{"start": 1, "end": 150, ...}]  | [{"key": "locus_tag", "values": ["cds0001"]}] |
    ----------------------------------------------------------------------------------------------------------------------------------------

    - source_path: nucleotide sequence file corresponding to this feature
    - seq_id: nucleotide sequence id (e.g., contig id) corresponding to this feature
    - type: feature's 'type' qualifier
    - location: compound locations of this feature
    - qualifiers: list of feature's flexible key-values pairs
    """
    
    @classmethod
    def _getSchemaSpecific(cls) -> T.StructType:
        return T.StructType([
            T.StructField("source_path", T.StringType()),
            T.StructField("seq_id", T.StringType()),
            T.StructField("type", T.StringType()),
            T.StructField("location", T.ArrayType(
                T.StructType([
                    T.StructField("start", T.LongType()),
                    T.StructField("end", T.LongType()),
                    T.StructField("start_partial", T.BooleanType()),
                    T.StructField("end_partial", T.BooleanType()),
                    T.StructField("strand", T.ByteType())
                ])
            )),
            T.StructField("qualifiers", T.ArrayType(
                T.StructType([
                    T.StructField("key", T.StringType()),
                    T.StructField("values", T.ArrayType(T.StringType()))
                ])
            ))
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return (
            len(row["location"]) > 0
        )

    @classmethod
    def getSimpleLocation(cls, compound_location: List[Dict]) -> Dict:
        """
        Given a compound location defined as list of location dictionaries,
        return a single location dictionary with the 'envelope' coordinates
        of the location (min-start and max-end). The compound location's strand
        will only follow the original, only if all of the compounded locations
        have the same strand, otherwise it will return '0' (i.e., unknown orientation).
        """
        
        if len(compound_location) < 1:
            return None
        return {
            "start": min([loc["start"] for loc in compound_location]),
            "end": max([loc["end"] for loc in compound_location]),
            "strand": compound_location[0]["strand"] if len(set([loc["strand"] for loc in compound_location])) == 1 else 0
        }