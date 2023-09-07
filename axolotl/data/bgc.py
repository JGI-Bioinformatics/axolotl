## implement a BGC df generator ##

"""axolotl.data.bgc

Contain classes definition for handling BGCs basic metadata (excluding actual CDSes)
"""

from pyspark.sql import Row, types, DataFrame
from typing import List, Dict
from axolotl.core import ioDF
from axolotl.data.feature import FeatDF
from axolotl.data.seq import NuclSeqDF

import pyspark.sql.functions as F

class bgcDF(ioDF):
    
    @classmethod
    def _getSchemaSpecific(cls) -> types.StructType:
        return types.StructType([
            types.StructField("source_path", types.StringType()),
            types.StructField("seq_id", types.StringType()),
            types.StructField("location", types.StructType([
                types.StructField("start", types.LongType()),
                types.StructField("end", types.LongType()),
                types.StructField("strand", types.LongType())
            ])),
            types.StructField("nt_seq", types.StringType()),
            types.StructField("classes", types.ArrayType(types.StringType())),
            types.StructField("on_contig_edge", types.BooleanType()),
            types.StructField("other_qualifiers", types.ArrayType(
                types.StructType([
                    types.StructField("key", types.StringType()),
                    types.StructField("values", types.ArrayType(types.StringType()))
                ])
            ))
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True

    @classmethod
    def fromFeatDF(cls, data: FeatDF, contigs: NuclSeqDF=None, source_type: str="antismash", reindex: bool=True):
        
        if source_type == "antismash":
            df = data.df.filter("type = 'region'")\
                .rdd.map(lambda row: Row(
                file_path=row.file_path,
                row_id=row.row_id,
                source_path=row.source_path,
                seq_id=row.seq_id,
                location={
                    "start": min([loc.start for loc in row.location]),
                    "end": max([loc.end for loc in row.location]),
                    "strand": row.location[0].strand if len(set([loc.strand for loc in row.location])) == 1 else 0
                },
                nt_seq=None,
                classes=([q.values for q in row.qualifiers if q.key == "product"][0:1] or [[]])[0],
                on_contig_edge=([q.values[0] == "True" for q in row.qualifiers if q.key == "contig_edge"][0:1] or [None])[0],
                other_qualifiers=[q for q in row.qualifiers if q.key not in ["product", "contig_edge"]]
            )).toDF(cls.getSchema()).filter((F.size(F.col("classes")) > 0))
        elif source_type == "smc":
            df = data.df.filter("type = 'cluster'")\
                .rdd.map(lambda row: Row(
                file_path=row.file_path,
                row_id=row.row_id,
                source_path=row.source_path,
                seq_id=row.seq_id,
                location={
                    "start": min([loc.start for loc in row.location]),
                    "end": max([loc.end for loc in row.location]),
                    "strand": row.location[0].strand if len(set([loc.strand for loc in row.location])) == 1 else 0
                },
                nt_seq=None,
                classes=([q.values for q in row.qualifiers if q.key == "BGC_Class"][0:1] or [[]])[0],
                on_contig_edge=None,
                other_qualifiers=[q for q in row.qualifiers if q.key not in ["BGC_Class", "contig_edge"]]
            )).toDF(cls.getSchema()).filter((F.size(F.col("classes")) > 0))

        if reindex:
            df = df.withColumn(
                "row_id", F.when(F.lit(True), F.monotonically_increasing_id())
            )
                
        if contigs is None: # return as is
            return cls(df)
        else: # given contigs df, also try grab nt_sequence data
            contig_df = contigs.df
            bgcs_lists = df.groupBy(["source_path", "seq_id"]).agg(
                F.collect_list("row_id").alias("row_ids"),
                F.collect_list("location").alias("locations")
            )
            joined = bgcs_lists.join(contig_df, [
                bgcs_lists.source_path == contig_df.file_path, bgcs_lists.seq_id == contig_df.seq_id
            ]).select(
                contig_df.sequence, bgcs_lists.row_ids, bgcs_lists.locations
            )
            
            sequences = joined.rdd.flatMap(
                lambda row: zip(row["row_ids"], [len(row["sequence"])]*len(row["locations"]), [
                    str(NuclSeqDF.fetch_seq(row["sequence"], loc)) for i, loc in enumerate(row["locations"])
                ])
            ).toDF(["row_id", "contig_nt_length", "seq"])

            joined = df.join(sequences, "row_id", "left").withColumn(
                "nt_seq",
                F.when(F.lit(True), sequences.seq)
            )

            if source_type in ["smc"]:
                # also calculate contig edges
                joined = joined.withColumn(
                    "on_contig_edge",
                    F.when(F.lit(True),
                        F.when(joined.contig_nt_length <= joined.location.end, F.lit(True)).otherwise(F.lit(False))
                    )
                )

            return cls(joined.select(df.columns))