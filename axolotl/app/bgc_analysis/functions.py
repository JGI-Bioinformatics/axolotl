from typing import List, Tuple, Dict
from pyhmmer.hmmer import hmmscan as pyhmmscan
from pyhmmer.plan7 import HMMFile
from pyhmmer.easel import TextSequence, Alphabet
from sklearn.cluster import Birch
from sklearn.neighbors import NearestNeighbors
import pandas as pd
from os import path
from glob import iglob
from math import sqrt
import warnings

from pyspark.sql import DataFrame, Row
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from axolotl.utils.file import fopen
from axolotl.utils.spark import activate_udf
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


def get_bigslice_features_column(bigslice_model_path: str):
    all_models = []
    # first, get all biosyn pfam models
    with open(path.join(bigslice_model_path, "biosynthetic_pfams", "Pfam-A.biosynthetic.hmm"), "r") as ii:
        for line in ii:
            if line.startswith("ACC "):
                all_models.append(line.rstrip("\n").split()[1])
    # then, get all subpfam models
    for subpfam_hmm in iglob(path.join(bigslice_model_path, "sub_pfams", "hmm", "*.hmm")):
        with open(subpfam_hmm, "r") as ii:
            for line in ii:
                if line.startswith("NAME "):
                    core_name, subpfam_number = (line.rstrip("\n").split()[1].split(".aligned_c"))
                    all_models.append("{}:{}".format(core_name, subpfam_number))
    return sorted(all_models)


def run_hmmscan(sequences: List[Tuple], hmm_db_path: str, num_cpus: int=1,
        bit_cutoff: float=None, cat_cutoff: str=None, include_only: List[str]=[],
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

    if len(include_only) > 0:
        hmm_file = [hmm for hmm in hmm_file if hmm.name.decode("utf-8") in include_only]
    hmm_profiles = {
        hmm.name: {
            "GA": hmm.cutoffs.gathering1 if hmm.cutoffs.gathering_available else None,
            "TC": hmm.cutoffs.trusted1 if hmm.cutoffs.trusted_available else None,
            "NC": hmm.cutoffs.noise1 if hmm.cutoffs.noise_available else None,
            "length": hmm.M
        } for hmm in hmm_file
    }
    sequences_pyhmmer = (
        TextSequence(
            name=bytes(idx, "utf-8"),
            sequence=aa_seq
        ).digitize(Alphabet.amino()) for idx, aa_seq in sequences
    )
    results = []
    for tophit in pyhmmscan(sequences_pyhmmer, hmm_file, cpus=num_cpus, T=bit_cutoff, bit_cutoffs=cat_cutoff):
        for hit in tophit:
            for domain in hit.domains:
                if cat_cutoff:
                    if cat_cutoff == "gathering":
                        if domain.score < hmm_profiles[hit.name]["GA"]:
                            continue
                    elif cat_cutoff == "trusted":
                        if domain.score < hmm_profiles[hit.name]["TC"]:
                            continue
                    elif cat_cutoff == "noise":
                        if domain.score < hmm_profiles[hit.name]["NC"]:
                            continue
                    else:
                        raise ValueError("cat_cutoff={} not recognized!".format(cat_cutoff))
                elif bit_cutoff:
                    if domain.score < bit_cutoff:
                        continue
                # else: standard e_cutoff=10.0                    
                results.append({
                    "query_name": tophit.query_name.decode("utf-8"),
                    "query_from": domain.alignment.target_from,
                    "query_to": domain.alignment.target_to,
                    "query_gaps": [i+1 for i, c in enumerate(str(domain.alignment.target_sequence)) if c == '-'],
                    "hmm_acc": (hit.accession or bytes("", "utf-8")).decode("utf-8"),
                    "hmm_name": (hit.name or bytes("", "utf-8")).decode("utf-8"),
                    "hmm_from": domain.alignment.hmm_from,
                    "hmm_to": domain.alignment.hmm_to,
                    "hmm_gaps":  [i+1 for i, c in enumerate(str(domain.alignment.hmm_sequence)) if c == '.'],
                    "bitscore": domain.score
                })
    return results


def scan_cdsDF(cds_df: cdsDF, hmm_db_path: str, num_cpus: int=1,
        bit_cutoff: float=None, cat_cutoff: str=None, include_only: List[str]=[],
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
            cat_cutoff=cat_cutoff,
            include_only=include_only,
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


def calc_bigslice_gcfs(
    input_df: DataFrame, bigslice_model_path: str, threshold: float
):
    """
    Calculate GCF centroids from BiG-SLiCE BGC vectors using Birch
    clustering algorithm. This function will take the pySpark DataFrame
    of the vectors, then perform a "mini-batch" approach on the Birch
    clustering algorithm, dividing the input DataFrame into partitions
    and calculate GCF features for each partition separately.
    
    # input_df schema: features (dict[string, int/float])
    # output df schema: idx (int), features(dict[string, float])
    """
    
    # first, get column headers information
    bigslice_vector_columns = get_bigslice_features_column(bigslice_model_path)
    
    def get_pandas_df_from_vectors(
        rows: List[Row], column_headers: List[str], vector_type:str="dict"
    ) -> pd.DataFrame:
        """
        Function to convert the original batch of pySpark DataFrame rows
        into Pandas DataFrame for sklearn processing
        """
    
        if vector_type == "dict":
            return pd.DataFrame.from_records(
                [row.features for row in rows],
                columns=column_headers
            ).fillna(0)
        else:
            raise Exception("vector type '{}' not supported.".format(vector_type))
    
    def run_birch(rows, threshold):
        """
        Function to call Birch clustering per partition
        """
        pandas_df = get_pandas_df_from_vectors(list(rows), bigslice_vector_columns)
        clusterer = Birch(
            threshold=threshold,
            branching_factor=pandas_df.shape[0],
            compute_labels=False,
            n_clusters=None
        )
        clusterer.fit(pandas_df)
        return clusterer.subcluster_centers_
    
    # run gcf calculation in Spark
    gcf_features = input_df.select(F.col("features")).rdd.mapPartitions(lambda rows: run_birch(rows, threshold))
    gcf_features = gcf_features.map(
        lambda vector: [pd.Series(vector, index=bigslice_vector_columns)[vector > 0].to_dict()]
    )
    gcf_features = gcf_features.toDF(T.StructType([
        T.StructField("features", T.MapType(T.StringType(), T.FloatType()))
    ])).select(
        F.monotonically_increasing_id().alias("gcf_id"),
        F.col("features")
    )
    
    return gcf_features


def apply_l2_norm(input_df: DataFrame, idx_colname: str="idx") -> DataFrame:
    """
    apply l2 normalization to a features DataFrame

    input_df schema: idx (int), features (dict[string, double])
    output df schema: idx (int), features(dict[string, double])

    use idx_colname to change the default 'idx' colname to something else
    """

    activate_udf("L2Normalize", T.MapType(T.StringType(), T.DoubleType()))
    return input_df.select(idx_colname, F.expr("L2Normalize(features)").alias("features"))


def get_gcf_membership(bgc_features, gcf_features, bigslice_db_path):
    """
    Run NearestNeighbor calculation (euclidean) to match bgc to gcf

    # bgc_features schema: bgc_id (int), features (dict[string, int/float])
    # gcf_features schema: gcf_id (int), features (dict[string, int/float])
    # output df schema: bgc_id (int), gcf_id (int), dist (float)
    """

    def _calc_nearest_neighbors(bgc_features_dict, gcf_features_dict, column_headers):
        if len(bgc_features_dict) < 1 or len(gcf_features_dict) < 1:
            return []
        bgc_ids, bgc_features = list(zip(*[(bgc_id, features) for bgc_id, features in bgc_features_dict.items()]))
        bgc_pd_df = pd.DataFrame.from_records(
            bgc_features,
            index=bgc_ids,
            columns=column_headers
        ).fillna(0)
        gcf_ids, gcf_features = list(zip(*[(gcf_id, features) for gcf_id, features in gcf_features_dict.items()]))
        gcf_pd_df = pd.DataFrame.from_records(
            gcf_features,
            index=gcf_ids,
            columns=column_headers
        ).fillna(0)
    
        nn = NearestNeighbors(
            metric='euclidean',
            algorithm='brute',
            n_jobs=1
        )
        nn.fit(gcf_pd_df.values)
        
        dists, centroids_idx = nn.kneighbors(
            X=bgc_pd_df.values,
            n_neighbors=1,
            return_distance=True
        )
    
        results = [
            [int(bgc_id), int(gcf_pd_df.index[centroids_idx[i][0]]), float(dists[i][0])]
            for i, bgc_id in enumerate(bgc_pd_df.index.values)
        ]
        return results

    ########

    column_headers = get_bigslice_features_column(bigslice_db_path)
    
    bgc_features_partitioned = bgc_features.rdd.mapPartitions(
        lambda rows: [[{row.bgc_id: row.features for row in rows}]]).toDF(
        T.StructType([
            T.StructField("bgc_features", T.MapType(T.LongType(), T.MapType(T.StringType(), T.DoubleType())))
        ])
    )

    gcf_features_partitioned = gcf_features.rdd.mapPartitions(
        lambda rows: [[{row.gcf_id: row.features for row in rows}]]).toDF(
        T.StructType([
            T.StructField("gcf_features", T.MapType(T.LongType(), T.MapType(T.StringType(), T.DoubleType())))
        ])
    )

    bgc_gcf_hits = bgc_features_partitioned.join(gcf_features_partitioned).rdd.flatMap(
        lambda chunk: _calc_nearest_neighbors(chunk.bgc_features, chunk.gcf_features, column_headers)
    ).toDF(
        T.StructType([
            T.StructField("bgc_id", T.LongType()),
            T.StructField("gcf_id", T.LongType()),
            T.StructField("dist", T.DoubleType())
        ])
    )

    w2 = Window.partitionBy("bgc_id").orderBy(F.col("dist").asc())
    return bgc_gcf_hits.withColumn("row", F.row_number().over(w2)) \
      .filter(F.col("row") == 1).drop("row")
