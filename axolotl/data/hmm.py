"""axolotl.data.hmm
"""

from pyspark.sql import Row, types, DataFrame
from typing import List, Dict
from axolotl.core import AxlDF
from axolotl.data.cds import cdsDF

from pyhmmer.hmmer import hmmscan as pyhmmscan
from pyhmmer.plan7 import HMMFile
from pyhmmer.easel import TextSequence, Alphabet
from pyspark.sql import Row

import pyspark.sql.functions as F
import pyspark.sql.types as T

import warnings

class hmmscanDF(AxlDF):
    
    @classmethod
    def getSchema(cls) -> T.StructType:
        return T.StructType([
            T.StructField("cds_id", T.LongType()),
            T.StructField("cds_from", T.LongType()),
            T.StructField("cds_to", T.LongType()),
            T.StructField("cds_gaps", T.ArrayType(T.LongType())),
            T.StructField("hmm_db_path", T.StringType()),
            T.StructField("hmm_acc", T.StringType()),
            T.StructField("hmm_name", T.StringType()),
            T.StructField("hmm_from", T.LongType()),
            T.StructField("hmm_to", T.LongType()),
            T.StructField("hmm_gaps", T.ArrayType(T.LongType())),
            T.StructField("bitscore", T.FloatType())
        ])
    
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True

    @classmethod
    def run_pyhmmscan(cls, rows: List[Row], hmm_db_path: str, num_cpus: int=1, T: float=None, E: float=10.0, use_cutoff: str=None):
        hmm_file = HMMFile(hmm_db_path).optimized_profiles()
        sequences_pyhmmer = (
            TextSequence(
                name=bytes(str(row["row_id"]), "utf-8"),
                sequence=row["aa_sequence"]
            ).digitize(Alphabet.amino()) for row in rows if row["aa_sequence"]
        )
        results = []
        for tophit in pyhmmscan(sequences_pyhmmer, hmm_file, cpus=num_cpus, T=T, E=E, bit_cutoffs=use_cutoff):
            for hit in tophit:
                results.append({
                    "cds_id": int(tophit.query_name),
                    "cds_from": hit.best_domain.alignment.target_from,
                    "cds_to": hit.best_domain.alignment.target_to,
                    "cds_gaps": [i+1 for i, c in enumerate(str(hit.best_domain.alignment.target_sequence)) if c == '-'],
                    "hmm_db_path": hmm_db_path,
                    "hmm_acc": hit.accession.decode("utf-8"),
                    "hmm_name": hit.name.decode("utf-8"),
                    "hmm_from": hit.best_domain.alignment.hmm_from,
                    "hmm_to": hit.best_domain.alignment.hmm_to,
                    "hmm_gaps":  [i+1 for i, c in enumerate(str(hit.best_domain.alignment.hmm_sequence)) if c == '.'],
                    "bitscore": hit.best_domain.score
                })
        return results

    @classmethod
    def scanCDS(cls, cds_df: cdsDF, hmm_db_path: str, num_cpus: int=1, T: float=None, E: float=10.0, use_cutoff: str=None):
        
        if use_cutoff:
            if use_cutoff not in ["NC", "GC" or "TC"]:
                raise Exception("use an appropriate pHMM cutoff ('NC', 'GC', or 'TC')")
            use_cutoff = {
                "GC": "gathering", "TC": "trusted", "NC": "noise"
            }[use_cutoff]
        
        return cls(
            cds_df.df.select(["row_id", "aa_sequence"]).rdd.flatMap(
                lambda row: cls.run_pyhmmscan(
                    [row], hmm_db_path,
                    num_cpus=num_cpus,
                    T=T,
                    E=E,
                    use_cutoff=use_cutoff
                )
            ).toDF(cls.getSchema())
        )