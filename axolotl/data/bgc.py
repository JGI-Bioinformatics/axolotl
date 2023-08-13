## implement a BGC df generator ##

"""axolotl.data.bgc

Contain classes definition for handling BGCs basic metadata (excluding actual CDSes)
"""

from pyspark.sql import Row, types, DataFrame
from typing import List, Dict
from axolotl.core import ioDF
from axolotl.data.feature import FeatDF

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
    def fromFeatDF(cls, data: FeatDF, source_type: str="antismash"):
        
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
                classes=([q.values for q in row.qualifiers if q.key == "product"][0:1] or [[]])[0],
                on_contig_edge=([q.values[0] == "True" for q in row.qualifiers if q.key == "contig_edge"][0:1] or [None])[0],
                other_qualifiers=[q for q in row.qualifiers if q.key not in ["product", "contig_edge"]]
            )).toDF(cls.getSchema())
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
                classes=([q.values for q in row.qualifiers if q.key == "BGC_Class"][0:1] or [[]])[0],
                on_contig_edge=([q.values[0] == "True" for q in row.qualifiers if q.key == "contig_edge"][0:1] or [None])[0],
                other_qualifiers=[q for q in row.qualifiers if q.key not in ["product", "contig_edge"]]
            )).toDF(cls.getSchema())
        
        return cls(df)