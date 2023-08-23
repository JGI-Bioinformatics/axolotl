## implement a CDS df generator ##

"""axolotl.data.cds

Contain classes definition for handling coding sequence data
"""

from pyspark.sql import Row, types, DataFrame
from axolotl.core import ioDF
from axolotl.data.feature import FeatDF
from axolotl.data.seq import NuclSeqDF
import pyspark.sql.functions as F

from Bio.Seq import Seq

class cdsDF(ioDF):
        
    @classmethod
    def _getSchemaSpecific(cls) -> types.StructType:
        return types.StructType([
            types.StructField("source_path", types.StringType()),
            types.StructField("seq_id", types.StringType()),
            types.StructField("locus_tag", types.StringType()),
            types.StructField("gene_name", types.StringType()),
            types.StructField("protein_name", types.StringType()),
            types.StructField("aa_sequence", types.StringType()),
            types.StructField("transl_table", types.StringType()),
            types.StructField("location", types.StructType([
                types.StructField("start", types.LongType()),
                types.StructField("end", types.LongType()),
                types.StructField("strand", types.LongType())
            ])),
            types.StructField("other_qualifiers", types.ArrayType(
                types.StructType([
                    types.StructField("key", types.StringType()),
                    types.StructField("values", types.ArrayType(types.StringType()))
                ])
            ))
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return row.aa_sequence != None

    @classmethod
    def fromFeatDF(cls, data: FeatDF, contigs: NuclSeqDF=None, reindex: bool=True):

        cds_df = data.df.filter("type = 'CDS'")\
            .rdd.map(lambda row: Row(
            file_path=row.file_path,
            row_id=row.row_id,
            source_path=row.source_path,
            seq_id=row.seq_id,
            locus_tag=([q.values[0] for q in row.qualifiers if q.key == "locus_tag"][0:1] or [None])[0],
            gene_name=([q.values[0] for q in row.qualifiers if q.key == "gene"][0:1] or [None])[0],
            protein_name=([q.values[0] for q in row.qualifiers if q.key == "product"][0:1] or [None])[0],
            aa_sequence=([q.values[0] for q in row.qualifiers if q.key == "translation"][0:1] or [None])[0],
            transl_table=([q.values[0] for q in row.qualifiers if q.key == "transl_table"][0:1] or [None])[0],
            location={
                "start": min([loc.start for loc in row.location]),
                "end": max([loc.end for loc in row.location]),
                "strand": row.location[0].strand if len(set([loc.strand for loc in row.location])) == 1 else 0
            },
            other_qualifiers=[q for q in row.qualifiers if q.key not in ["locus_tag", "gene", "product", "translation", "transl_table"]]
        )).withColumn(
            "row_id", F.monotonically_increasing_id()
        ).toDF(cls.getSchema())
        
        if contigs is None: # return as is
            return cls(cds_df)
        else: # given contigs df, also try to translate missing CDS translations
            cds_df.persist()
            cds_df.count()
            contig_df = contigs.df
            missing_cds = cds_df.filter("aa_sequence is NULL").fillna("", ["transl_table"]).groupBy(["source_path", "seq_id"]).agg(
                F.collect_list("row_id").alias("row_ids"),
                F.collect_list("location").alias("locations"),
                F.collect_list("transl_table").alias("transl_tables")
            )
            joined = missing_cds.join(contig_df, [missing_cds.source_path == contig_df.file_path, missing_cds.seq_id == contig_df.seq_id]).select(contig_df.sequence, missing_cds.row_ids, missing_cds.locations, missing_cds.transl_tables)
            
            translated = joined.rdd.flatMap(
                lambda row: zip(row["row_ids"], [
                    str(Seq.translate(
                        NuclSeqDF.fetch_seq(row["sequence"], loc),
                        table = int(row["transl_tables"][i]) if row["transl_tables"][i] else "Standard"
                    ).rstrip("*")) for i, loc in enumerate(row["locations"])
                ])
            ).toDF(["row_id", "seq"])

            return cls(
                cds_df.join(translated, "row_id", "left").withColumn(
                    "aa_sequence",
                    F.when(cds_df.aa_sequence.isNotNull(), cds_df.aa_sequence).otherwise(translated.seq)
                ).select(cds_df.columns)
            )
