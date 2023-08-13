## implement a CDS df generator ##

"""axolotl.data.cds

Contain classes definition for handling coding sequence data
"""

from pyspark.sql import Row, types, DataFrame
from axolotl.core import ioDF
from axolotl.data.feature import FeatDF


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
    def fromFeatDF(cls, data: FeatDF):
        
        df = data.df.filter("type = 'CDS'")\
            .rdd.map(lambda row: Row(
            file_path=row.file_path,
            row_id=row.row_id,
            source_path=row.source_path,
            seq_id=row.seq_id,
            locus_tag=([q.values[0] for q in row.qualifiers if q.key == "locus_tag"][0:1] or [None])[0],
            gene_name=([q.values[0] for q in row.qualifiers if q.key == "gene"][0:1] or [None])[0],
            protein_name=([q.values[0] for q in row.qualifiers if q.key == "product"][0:1] or [None])[0],
            aa_sequence=([q.values[0] for q in row.qualifiers if q.key == "translation"][0:1] or [None])[0],
            location={
                "start": min([loc.start for loc in row.location]),
                "end": max([loc.end for loc in row.location]),
                "strand": row.location[0].strand if len(set([loc.strand for loc in row.location])) == 1 else 0
            },
            other_qualifiers=[q for q in row.qualifiers if q.key not in ["locus_tag", "gene", "product", "translation"]]
        )).toDF(cls.getSchema())
        
        return cls(df)