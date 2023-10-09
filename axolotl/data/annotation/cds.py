import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row
from Bio.Seq import Seq

from axolotl.data import ioDF
from axolotl.data.annotation.base import RawFeatDF
from axolotl.data.sequence import NuclSeqDF

from typing import Dict, List


class cdsDF(ioDF):
    """
    AxlDF subclass to represent CDS (coding sequence) features in a genome annotation data. This DataFrame is typically
    (but not always) derived from a raw features DataFrame (RawFeatDF). As opposed to RawFeatDF, the "location" column
    in a cdsDF is stored as a "SimpleLocation" dict.

    Example DataFrame content:

    --------------------------------------------------------------------------------------------------------------------------
    | idx | file_path  | source_path | seq_id   | location                         | locus_tag  | gene_name  | protein_name |
    --------------------------------------------------------------------------------------------------------------------------
    | 1   | /test.gff  | /test.fa    | contig_1 | {"start": 1, "end": 150, ...}    | CDS_0001   | acdX       | None         |
    | 2   | /test.gff  | /test.fa    | contig_1 | {"start": 255, "end": 700, ...}  | CDS_0002   | None       | None         |
    --------------------------------------------------------------------------------------------------------------------------
    -------------------------------------------------------------------
     aa_sequence                 | transl_table  | other_qualifiers   |
    -------------------------------------------------------------------
     MSNEEKLVGYLKRVTADLHETRE...  | 11            | []                 |
     AMDPQQRLLLETSWEALERAGIS...  | 11            | []                 |
    -------------------------------------------------------------------

    - source_path: nucleotide sequence file corresponding to this CDS
    - seq_id: nucleotide sequence id (e.g., contig id) corresponding to this CDS
    - location: { start, end, strand } location of this CDS in the sequence
    - locus_tag, gene_name, protein_name: self-explanatory
    - transl_table: translation table type, will be passed down directly to BioPython's Seq.translate() function
    - other_qualifiers: the rest of the qualifiers column after taking out locus_tag, gene_name, protein_name and transl_table
    """

    @classmethod
    def _getSchemaSpecific(cls) -> T.StructType:
        return T.StructType([
            T.StructField("source_path", T.StringType()),
            T.StructField("seq_id", T.StringType()),
            T.StructField("location", T.StructType([
                T.StructField("start", T.LongType()),
                T.StructField("end", T.LongType()),
                T.StructField("strand", T.ByteType())
            ])),
            T.StructField("locus_tag", T.StringType()),
            T.StructField("gene_name", T.StringType()),
            T.StructField("protein_name", T.StringType()),
            T.StructField("aa_sequence", T.StringType()),
            T.StructField("transl_table", T.ByteType()),
            T.StructField("other_qualifiers", T.ArrayType(
                T.StructType([
                    T.StructField("key", T.StringType()),
                    T.StructField("values", T.ArrayType(T.StringType()))
                ])
            ))
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return (
            row.aa_sequence is not None
        )

    @classmethod
    def translate_seq(cls, seq: str, transl_table: int=None):
        """
        use BioPython's Seq.translate() function to translate a snippet of a nucleotide sequence
        given a standard translation table.
        """
        try:
            return str(
                Seq.translate(seq, transl_table if transl_table else "Standard")
            ).rstrip("*")
        except:
            return None

    @classmethod
    def fromRawFeatDF(cls, features: RawFeatDF, reindex: bool=True):
        """
        the primary class method to use for generating a cdsDF given a previously-parsed RawFeatDF.
        This method will filter all rows in the RawFeatDF with type=='CDS' and extract CDS-specific
        columns from the original qualifier columns. Compound locations will be merged into a SimpleLocation,
        and the method will try to translate amino acid sequences, given that a NuclSeqDF is provided along
        with the RawFeatDF, and that the 'aa_sequence' qualifier in the original feature is None. Remaining
        feature qualifiers will be stored as 'other_qualifiers'.

        By default, the resulting cdsDF will have its own 'idx' recalculated (reindex=True).
        """

        cds_df = features.df.filter("type = 'CDS'")\
            .rdd.map(lambda row: {
                "idx" : row.idx,
                "file_path" : row.file_path,
                "source_path" : row.source_path,
                "seq_id" : row.seq_id,
                "location" : RawFeatDF.getSimpleLocation(row.location),
                "locus_tag" : ([q.values[0] for q in row.qualifiers if q.key == "locus_tag"][0:1] or [None])[0],
                "gene_name" : ([q.values[0] for q in row.qualifiers if q.key == "gene"][0:1] or [None])[0],
                "protein_name" : ([q.values[0] for q in row.qualifiers if q.key == "product"][0:1] or [None])[0],
                "aa_sequence" : ([q.values[0] for q in row.qualifiers if q.key == "translation"][0:1] or [None])[0],
                "transl_table" : ([int(q.values[0]) for q in row.qualifiers if q.key == "transl_table" and q.values[0].isnumeric()][0:1] or [None])[0],
                "other_qualifiers" : [q for q in row.qualifiers if q.key not in [
                    "locus_tag", "gene", "product", "translation", "transl_table"
                ]]
        }).toDF(cls.getSchema())

        return cls(cds_df, override_idx=reindex, keep_idx=(not reindex), sources=[features])

    def translateAAs(self, sequences: NuclSeqDF, translate_all: bool=False):
        """
        Given the original NuclSeqDF (i.e., contig sequences), try to translate missing AAs in the cds.
        If translate_all = True, it will override existing AAs also.
        """

        cds_df = self.df
        
        missing_cds = cds_df
        if not translate_all:
            missing_cds = missing_cds.filter("aa_sequence is NULL")
        missing_cds = missing_cds.fillna(-1, ["transl_table"]).groupBy(["source_path", "seq_id"]).agg(
            F.collect_list("idx").alias("row_ids"),
            F.collect_list("location").alias("locations"),
            F.collect_list("transl_table").alias("transl_tables")
        )
        joined = missing_cds.join(sequences.df, [missing_cds.source_path == sequences.df.file_path, missing_cds.seq_id == sequences.df.seq_id])\
            .select(sequences.df.sequence, missing_cds.row_ids, missing_cds.locations, missing_cds.transl_tables)
        translated = joined.rdd.flatMap(
            lambda row: zip(row.row_ids, [
                (cdsDF.translate_seq(
                    NuclSeqDF.fetch_seq(row.sequence, loc),
                    row.transl_tables[i]
                )) for i, loc in enumerate(row.locations)
            ])
        ).toDF(T.StructType([
            T.StructField("idx", T.LongType()),
            T.StructField("seq", T.StringType())
        ]))

        df = cds_df.join(translated, "idx", "left").withColumn(
            "aa_sequence",
            F.when(cds_df.aa_sequence.isNotNull(), cds_df.aa_sequence).otherwise(translated.seq)
        ).select(self.__class__.getSchema().fieldNames())

        new_obj = self.__class__(df, override_idx=False, keep_idx=True)
        new_obj.updateSourcesByIds([self._sources[0], sequences._id])

        return new_obj
