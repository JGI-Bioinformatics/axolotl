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
            T.StructField("query_name", T.StringType()),
            T.StructField("query_from", T.LongType()),
            T.StructField("query_to", T.LongType()),
            T.StructField("query_gaps", T.ArrayType(T.LongType())),
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
    def run_pyhmmscan(
        cls, rows: List[Row], hmm_db_path: str, num_cpus: int=1,
        T: float=None, E: float=10.0, use_cutoff: str=None,
        use_unoptimized_hmm: bool=False
    ):
        hmm_file = HMMFile(hmm_db_path)
        try:
            hmm_file = hmm_file.optimized_profiles()
        except:
            if use_unoptimized_hmm:
                warnings.warn("trying to use an unoptimized HMM profile ({})!!".format(hmm_db_path))
            else:
                raise Exception("trying to use an unoptimized HMM profile ({})!! Set 'use_unoptimized_hmm'=True to ignore this error.".format(hmm_db_path))
        sequences_pyhmmer = (
            TextSequence(
                name=bytes(row["name"], "utf-8"),
                sequence=row["aa_sequence"]
            ).digitize(Alphabet.amino()) for row in rows if row["aa_sequence"]
        )
        results = []
        for tophit in pyhmmscan(sequences_pyhmmer, hmm_file, cpus=num_cpus, T=T, E=E, bit_cutoffs=use_cutoff):
            for hit in tophit:
                results.append({
                    "query_name": tophit.query_name.decode("utf-8"),
                    "query_from": hit.best_domain.alignment.target_from,
                    "query_to": hit.best_domain.alignment.target_to,
                    "query_gaps": [i+1 for i, c in enumerate(str(hit.best_domain.alignment.target_sequence)) if c == '-'],
                    "hmm_db_path": hmm_db_path,
                    "hmm_acc": (hit.accession or bytes("", "utf-8")).decode("utf-8"),
                    "hmm_name": (hit.name or bytes("", "utf-8")).decode("utf-8"),
                    "hmm_from": hit.best_domain.alignment.hmm_from,
                    "hmm_to": hit.best_domain.alignment.hmm_to,
                    "hmm_gaps":  [i+1 for i, c in enumerate(str(hit.best_domain.alignment.hmm_sequence)) if c == '.'],
                    "bitscore": hit.best_domain.score
                })
        return results

    @classmethod
    def scanCDS(cls, cds_df: cdsDF, hmm_db_path: str, num_cpus: int=1, bit_cutoff: float=None, e_cutoff: float=10.0, phmm_cutoff: str=None):
        
        if phmm_cutoff:
            if phmm_cutoff not in ["NC", "GC" or "TC"]:
                raise Exception("use an appropriate pHMM cutoff ('NC', 'GC', or 'TC')")
            phmm_cutoff = {
                "GC": "gathering", "TC": "trusted", "NC": "noise"
            }[phmm_cutoff]
        
        return cls(
            cds_df.df.select(F.col("row_id").cast(T.StringType()).alias("name"), "aa_sequence").rdd.flatMap(
                lambda row: cls.run_pyhmmscan(
                    [row], hmm_db_path,
                    num_cpus=num_cpus,
                    T=bit_cutoff,
                    E=e_cutoff,
                    use_cutoff=phmm_cutoff
                )
            ).toDF(cls.getSchema())
        )