from typing import List, Tuple, Dict
from pyhmmer.hmmer import hmmscan as pyhmmscan
from pyhmmer.plan7 import HMMFile
from pyhmmer.easel import TextSequence, Alphabet
from os import path
import warnings

from pyspark.sql import DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F

from axolotl.utils.file import fopen
from axolotl.data.annotation import cdsDF


def scan_bigslice_db_folder(bigslice_db_path: str):
    """
    """

    biopfam_md5_path = path.join(bigslice_db_path, "biosynthetic_pfams", "biopfam.md5sum")
    with fopen(biopfam_md5_path, "r") as file_stream:
        biopfam_md5 = file_stream.read().rstrip("\n")
    subpfam_md5_path = path.join(bigslice_db_path, "sub_pfams", "corepfam.md5sum")
    with fopen(subpfam_md5_path, "r") as file_stream:
        subpfam_md5 = file_stream.read().rstrip("\n")
    return (biopfam_md5, subpfam_md5)


def run_hmmscan(sequences: List[Tuple], hmm_db_path: str, num_cpus: int=1,
        bit_cutoff: float=None, e_cutoff: float=10.0, cat_cutoff: str=None,
        use_unoptimized_hmm: bool=False) -> List[Dict]:
    """
    """

    hmm_file = HMMFile(hmm_db_path)
    try:
        hmm_file = hmm_file.optimized_profiles()
    except ValueError:
        if use_unoptimized_hmm:
            warnings.warn("trying to use an unoptimized HMM profile ({})!!".format(hmm_db_path))
        else:
            raise ValueError("trying to use an unoptimized HMM profile ({})!! Set 'use_unoptimized_hmm'=True to ignore this error.".format(hmm_db_path))
    sequences_pyhmmer = (
        TextSequence(
            name=bytes(idx, "utf-8"),
            sequence=aa_seq
        ).digitize(Alphabet.amino()) for idx, aa_seq in sequences
    )
    results = []
    for tophit in pyhmmscan(sequences_pyhmmer, hmm_file, cpus=num_cpus, T=bit_cutoff, E=e_cutoff, bit_cutoffs=cat_cutoff):
        for hit in tophit:
            results.append({
                "query_name": tophit.query_name.decode("utf-8"),
                "query_from": hit.best_domain.alignment.target_from,
                "query_to": hit.best_domain.alignment.target_to,
                "query_gaps": [i+1 for i, c in enumerate(str(hit.best_domain.alignment.target_sequence)) if c == '-'],
                "hmm_acc": (hit.accession or bytes("", "utf-8")).decode("utf-8"),
                "hmm_name": (hit.name or bytes("", "utf-8")).decode("utf-8"),
                "hmm_from": hit.best_domain.alignment.hmm_from,
                "hmm_to": hit.best_domain.alignment.hmm_to,
                "hmm_gaps":  [i+1 for i, c in enumerate(str(hit.best_domain.alignment.hmm_sequence)) if c == '.'],
                "bitscore": hit.best_domain.score
            })
    return results


def scan_cdsDF(cds_df: cdsDF, hmm_db_path: str, num_cpus: int=1,
        bit_cutoff: float=None, e_cutoff: float=10.0, cat_cutoff: str=None,
        use_unoptimized_hmm: bool=False) -> DataFrame:
    """
    """

    result_df = cds_df.df.filter(
        F.col("aa_sequence").isNotNull()
    ).select(
        F.col("idx").cast(T.StringType()).alias("name"),
        "aa_sequence"
    ).rdd.flatMap(
        lambda row: run_hmmscan(
            [(row.name, row.aa_sequence)], hmm_db_path,
            num_cpus=num_cpus,
            bit_cutoff=bit_cutoff,
            e_cutoff=e_cutoff,
            cat_cutoff=cat_cutoff,
            use_unoptimized_hmm=use_unoptimized_hmm
        )
    ).toDF()

    result_df = result_df.select(
        result_df.query_name.cast(T.LongType()).alias("cds_id"),
        result_df.query_from.alias("cds_from"),
        result_df.query_to.alias("cds_to"),
        result_df.query_gaps.alias("cds_gaps"),
        "hmm_acc", "hmm_name", "hmm_from", "hmm_to", "hmm_gaps", "bitscore"
    )

    return result_df