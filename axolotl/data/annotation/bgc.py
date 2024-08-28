import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row
from Bio.Seq import Seq

from axolotl.data import ioDF
from axolotl.data.annotation.base import RawFeatDF
from axolotl.data.sequence import NuclSeqDF

from typing import Dict, List


class bgcDF(ioDF):
    """
    AxlDF subclass to represent BGC (biosynthetic gene cluster) features in a genome annotation data. BGCs are usually stored
    in a GenBank-formatted file (or in some cases GFF3) and have their own set of BGC-specific qualifiers. Many tools provide
    identification of BGCs from genomes, the most widely-used one being antiSMASH (https://antismash.secondarymetabolites.org/#!/start).

    This AxlDF subclass aims to standardized the information of a BGC (e.g., what columns does it have). Typically a bgcDF table
    will need to be accompanied by a cdsDF table, which holds the information of the actual genes included in each BGC, linked
    by the overlaps between their locations.

    Example DataFrame content:

    --------------------------------------------------------------------------------------------------------------------------
    | idx | file_path  | source_path | seq_id   | location                            | classes         | nt_seq  
    --------------------------------------------------------------------------------------------------------------------------
    | 1   | /test.gff  | /test.fa    | contig_1 | {"start": 1, "end": 30000, ...}     | ["NRPS"]        | ATGCATGCATGC...
    | 2   | /test.gff  | /test.fa    | contig_1 | {"start": 45600, "end": 72300, ...} | ["PKS", "NRPS"] | GCATATGCATGC...
    --------------------------------------------------------------------------------------------------------------------------
    -------------------------------------------------------------------
     on_contig_edge  | other_qualifiers
    -------------------------------------------------------------------
     True            | [{"cluster-number": "1"}]
     False           | [{"cluster-number": "2"}]
    -------------------------------------------------------------------

    - source_path: nucleotide sequence file corresponding to this BGC
    - seq_id: nucleotide sequence id (e.g., contig id) corresponding to this BGC
    - location: { start, end, strand } location of this BGC in the sequence
    - nt_seq: nucleotide sequence of this BGC (only available if you supply a NuclSeqDF alongside the RawFeatDF)
    - on_contig_edge: whether the BGC sits in the boundary of a contig (i.e., a fragmented BGC)
    - other_qualifiers: the rest of the qualifiers column after taking out the classes and contig_edge information
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
            T.StructField("classes", T.ArrayType(T.StringType())),
            T.StructField("nt_seq", T.StringType()),
            T.StructField("on_contig_edge", T.BooleanType()),
            T.StructField("other_qualifiers", T.ArrayType(
                T.StructType([
                    T.StructField("key", T.StringType()),
                    T.StructField("values", T.ArrayType(T.StringType()))
                ])
            ))
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True

    @classmethod
    def fromRawFeatDF(cls, features: RawFeatDF, source_type: str="antismash", reindex: bool=False):
        """
        the primary class method to use for generating a bgcDF given a previously-parsed RawFeatDF. By default, it will parse
        for antiSMASH-type BGCs (encoded as a "region" in the gbk file). Use source_type == "smc" if the BGC features come
        from the SMC database's (https://smc.jgi.doe.gov/) GFF3 files.

        By default, the resulting bgcDF will retain the original 'idx' of the feature (reindex=False).
        """
        
        if source_type == "antismash":
            df = features.df.filter("type = 'region'")\
                .rdd.map(lambda row: {
                    "idx" : row.idx,
                    "file_path" : row.file_path,
                    "source_path" :row.source_path,
                    "seq_id" : row.seq_id,
                    "location" : RawFeatDF.getSimpleLocation(row.location),
                    "nt_seq" : None,
                    "classes" : ([q.values for q in row.qualifiers if q.key == "product"][0:1] or [[]])[0],
                    "on_contig_edge" : ([q.values[0] == "True" for q in row.qualifiers if q.key == "contig_edge"][0:1] or [None])[0],
                    "other_qualifiers" : [q for q in row.qualifiers if q.key not in ["product", "contig_edge"]]
            }).toDF(cls.getSchema()).filter((F.size(F.col("classes")) > 0)) # to exclude 'region' features that are not a BGC

        elif source_type == "smc":
            df = features.df.filter("type = 'cluster'")\
                .rdd.map(lambda row: {
                    "idx" : row.idx,
                    "file_path" : row.file_path,
                    "source_path" : row.source_path,
                    "seq_id" : row.seq_id,
                    "location" : RawFeatDF.getSimpleLocation(row.location),
                    "nt_seq" : None,
                    "classes" : ([q.values for q in row.qualifiers if q.key == "BGC_Class"][0:1] or [[]])[0],
                    "on_contig_edge" : None,
                    "other_qualifiers" : [q for q in row.qualifiers if q.key not in ["BGC_Class", "contig_edge"]]
            }).toDF(cls.getSchema()).filter((F.size(F.col("classes")) > 0)) # to exclude 'cluster' features that are not a BGC

        else:
            raise Exception("unrecognized source_type: '{}'".format(source_type))

        return cls(df, override_idx=reindex, keep_idx=(not reindex), sources=[features])
                

    def fillSeqInfo(self, sequences: NuclSeqDF, check_contig_edges: bool=False):
        """
        When a NuclSeqDF is also supplied, Axolotl will exract the nucleotide sequences information
        of each BGC, along with the "on_contig_edge" status (if check_contig_edges=True).
        """

        df = self.df
        bgcs_lists = df.groupBy(["source_path", "seq_id"]).agg(
            F.collect_list("idx").alias("row_ids"),
            F.collect_list("location").alias("locations")
        )
        joined = bgcs_lists.join(sequences.df, [
            bgcs_lists.source_path == sequences.df.file_path, bgcs_lists.seq_id == sequences.df.seq_id
        ]).select(
            sequences.df.sequence, bgcs_lists.row_ids, bgcs_lists.locations
        )
        bgc_sequences = joined.rdd.flatMap(
            lambda row: zip(row["row_ids"], [len(row["sequence"])]*len(row["locations"]), [
                str(NuclSeqDF.fetch_seq(row["sequence"], loc)) for i, loc in enumerate(row["locations"])
            ])
        ).toDF(T.StructType([
            T.StructField("idx", T.LongType()),
            T.StructField("contig_nt_length", T.LongType()),
            T.StructField("seq", T.StringType())
        ]))
        joined = df.join(bgc_sequences, "idx", "left").withColumn(
            "nt_seq",
            F.when(F.lit(True), bgc_sequences.seq)
        )
        if check_contig_edges:
            # fill contig edges information
            joined = joined.withColumn(
                "on_contig_edge",
                F.when(F.lit(True),
                    F.when(joined.contig_nt_length <= joined.location.end, F.lit(True)).otherwise(F.lit(False))
                )
            )
        df = joined.select(self.__class__.getSchema().fieldNames())
        
        new_obj = self.__class__(joined, override_idx=False, keep_idx=True)
        new_obj.updateSourcesByIds([self._sources[0], sequences._id])

        return new_obj