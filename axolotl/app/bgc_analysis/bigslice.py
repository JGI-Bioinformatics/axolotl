import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W
from pyspark.sql import DataFrame

from axolotl.app.base import AxlApp
from axolotl.data import RelationDF
from axolotl.data.sequence import NuclSeqDF
from axolotl.data.annotation import RawFeatDF, bgcDF, cdsDF
from axolotl.utils.file import check_file_exists, make_dirs
from axolotl.utils.spark import get_spark_session_and_context
from axolotl.app.bgc_analysis.functions import (
    scan_bigslice_db_folder, scan_cdsDF, run_hmmscan,
    calc_bigslice_gcfs, apply_l2_norm, get_gcf_membership
)

from typing import Dict
from os import path


class BigsliceApp(AxlApp):

    @classmethod
    def _dataDesc(cls) -> Dict:
        return {
            "bgc": bgcDF,
            "cds": cdsDF,
            "cds_to_bgc": RelationDF
        }

    def _creationFunc(self, features: RawFeatDF, source_type: str="antismash", kw_type: str=None, kw_classes: str=None):
        """
        extract bgc_df, cds_df and build cds_to_bgc dataframe that will act as BigsliceApp's core data
        """

        print("Extracting & saving bgcDF from RawFeatDF...")
        bgc_df = bgcDF.fromRawFeatDF(features, source_type = source_type, kw_type = kw_type, kw_classes = kw_classes)
        self._setData("bgc", bgc_df)
        self._saveData("bgc")
        bgc_df = self._getData("bgc")  

        print("Calculating cdsDF and cds_to_bgc table...")
        cds_df = cdsDF.fromRawFeatDF(features)
        joined = bgc_df.df.alias("bgc").join(cds_df.df.alias("cds"), [
            F.col("bgc.source_path") == F.col("cds.source_path"),
            F.col("bgc.seq_id") == F.col("cds.seq_id"),
            F.col("bgc.location.start") <= F.col("cds.location.start"),
            F.col("bgc.location.end") >= F.col("cds.location.end")
        ])\
            .withColumn("cds.idx", F.monotonically_increasing_id())\
            .select("cds.*", F.col("bgc.idx").alias("bgc_id"))
        joined.persist() # need to persist so we don't recalculate cdsDF
        joined.count()

        print("Saving cdsDF...")
        cds_df = cdsDF(
            joined.select(cdsDF.getSchema().fieldNames()),
            keep_idx=True,
            sources = [features]
        )
        self._setData("cds", cds_df)
        self._saveData("cds")
        cds_df = self._getData("cds")

        print("Saving cds_to_bgc table...")
        cds_bgc_df = RelationDF(joined.select(
            F.col("idx").alias("idx_1"),
            F.col("bgc_id").alias("idx_2")
        ), sources = [cds_df, bgc_df])
        self._setData("cds_to_bgc", cds_bgc_df)
        self._saveData("cds_to_bgc")
        cds_bgc_df = self._getData("cds_to_bgc")

    def _loadExtraData(self):
        return

    ### BigsliceApp-specific functions ###


    def getBGCMembership(self,
        bigslice_db_path: str, top_k: int=3, threshold: float=0.4,
        partition_num: int=None, bgcs_per_chunk: int=4000, gcfs_per_chunk: int=4000):

        spark, sc = get_spark_session_and_context()

        # first, read bigslice_db folder to get the unique db ids
        biopfam_md5, subpfam_md5 = scan_bigslice_db_folder(bigslice_db_path)
        # fetch current bgc, cds and cds_to_bgc ids
        bgc_df_id = self._getData("bgc")._id
        cds_df_id = self._getData("cds")._id
        link_df_id = self._getData("cds_to_bgc")._id

        # create feature folder if not exist
        feature_folder = path.join(
            self._folder_path, "features",
            "{}-{}-{}".format(
                bgc_df_id.split("#")[1],
                cds_df_id.split("#")[1],
                link_df_id.split("#")[1]
            )
        )
        if not check_file_exists(feature_folder):
            make_dirs(feature_folder)

        p = partition_num
        if not p:
            p = input_features_df.rdd.getNumPartitions()

        # check if gcf centroids are calculated
        bgc_membership_filename = "membership-p{}-t{}-k{}-{}-{}".format(
            p, threshold, top_k, biopfam_md5, subpfam_md5
        )
        membership_pq_path = path.join(feature_folder, bgc_membership_filename)

        if not check_file_exists(membership_pq_path):
            bgc_vectors = self.getBGCVectors(bigslice_db_path, top_k=top_k)
            gcf_centroids = self.getGCFCentroids(
                bigslice_db_path, top_k=top_k, threshold=threshold, partition_num=partition_num
            )
            
            print("calculating BGC-to-GCF membership... t={}, p={}".format(threshold, partition_num))

            get_gcf_membership(
                apply_l2_norm(bgc_vectors, "bgc_id").repartition(bgc_vectors.count() // bgcs_per_chunk),
                gcf_centroids.repartition(gcf_centroids.count() // gcfs_per_chunk),
                bigslice_db_path
            ).write.parquet(membership_pq_path)

        else:
            print("fetching BGC-to-GCF membership... t={}, p={}".format(threshold, partition_num))

        return spark.read.parquet(membership_pq_path)


    def getGCFCentroids(self, bigslice_db_path: str, top_k: int=3, threshold: float=0.4, partition_num: int=None):

        spark, sc = get_spark_session_and_context()

        # first, read bigslice_db folder to get the unique db ids
        biopfam_md5, subpfam_md5 = scan_bigslice_db_folder(bigslice_db_path)
        # fetch current bgc, cds and cds_to_bgc ids
        bgc_df_id = self._getData("bgc")._id
        cds_df_id = self._getData("cds")._id
        link_df_id = self._getData("cds_to_bgc")._id

        # create feature folder if not exist
        feature_folder = path.join(
            self._folder_path, "features",
            "{}-{}-{}".format(
                bgc_df_id.split("#")[1],
                cds_df_id.split("#")[1],
                link_df_id.split("#")[1]
            )
        )
        if not check_file_exists(feature_folder):
            make_dirs(feature_folder)

        input_features_df = apply_l2_norm(self.getBGCVectors(bigslice_db_path, top_k=top_k), "bgc_id")
        if not partition_num:
            partition_num = input_features_df.rdd.getNumPartitions()
        elif partition_num > 0:
            input_features_df = input_features_df.repartition(partition_num)

        # check if gcf centroids are calculated
        gcf_centroids_filename = "gcf-p{}-t{}-k{}-{}-{}".format(
            partition_num, threshold, top_k, biopfam_md5, subpfam_md5
        )
        gcf_pq_path = path.join(feature_folder, gcf_centroids_filename)

        if not check_file_exists(gcf_pq_path):
            print("calculating GCF centroids... t={}, p={}".format(threshold, partition_num))
            calc_bigslice_gcfs(
                input_features_df, bigslice_db_path, threshold
            ).write.parquet(gcf_pq_path)
        else:
            print("fetching GCF centroids... t={}, p={}".format(threshold, partition_num))

        return spark.read.parquet(gcf_pq_path)


    def getBGCVectors(self, bigslice_db_path: str, top_k: int=3) -> DataFrame:

        spark, sc = get_spark_session_and_context()

        # first, read bigslice_db folder to get the unique db ids
        biopfam_md5, subpfam_md5 = scan_bigslice_db_folder(bigslice_db_path)

        # fetch current bgc, cds and cds_to_bgc ids
        bgc_df_id = self._getData("bgc")._id
        cds_df_id = self._getData("cds")._id
        link_df_id = self._getData("cds_to_bgc")._id

        # create feature folder if not exist
        feature_folder = path.join(
            self._folder_path, "features",
            "{}-{}-{}".format(
                bgc_df_id.split("#")[1],
                cds_df_id.split("#")[1],
                link_df_id.split("#")[1]
            )
        )
        if not check_file_exists(feature_folder):
            make_dirs(feature_folder)

        # check if features are extracted
        feature_pq_path = path.join(feature_folder, "feat-k{}-{}-{}".format(
            top_k, biopfam_md5, subpfam_md5
        ))
        if not check_file_exists(feature_pq_path):

            # check if already biopfam-scanned
            biopfam_pq_path = path.join(feature_folder, "biopfam-{}".format(
                biopfam_md5
            ))
            if not check_file_exists(biopfam_pq_path):
                print("running biopfam-scan...")
                # run biopfam scan and save parquet
                biopfam_model_path = path.join(
                    bigslice_db_path, "biosynthetic_pfams", "Pfam-A.biosynthetic.hmm"
                )
                biopfam_scan_df = scan_cdsDF(
                    self._getData("cds"), biopfam_model_path,
                    cat_cutoff="gathering", use_unoptimized_hmm=True
                ).select(
                    "cds_id", "cds_from", "cds_to", "hmm_acc"
                )
                biopfam_scan_df.write.parquet(biopfam_pq_path)
            else:
                print("fetching biopfam-scan result...")

            # check if already subpfam-scanned
            subpfam_pq_path = path.join(feature_folder, "subpfam-{}-{}".format(
                biopfam_md5, subpfam_md5
            ))
            if not check_file_exists(subpfam_pq_path):
                print("running subpfam-scan...")
                biopfam_scan_df = spark.read.parquet(biopfam_pq_path)
                with open(path.join(bigslice_db_path, "sub_pfams", "corepfam.tsv")) as file_stream:
                    file_stream.readline()
                    list_corepfams = [line.split("\t")[1] for line in file_stream]

                # select only core biopfam_scan hits and fetch the corresponding sequences
                # (by taking the substring of the original CDS sequences)
                cds_df = self._getData("cds").df
                input_df = biopfam_scan_df.filter(F.col("hmm_acc").isin(list_corepfams))\
                    .withColumn("subpfam_hmm_path", F.udf(lambda acc: path.join(
                        bigslice_db_path, "sub_pfams", "hmm", acc + ".subpfams.hmm"
                    ), T.StringType())("hmm_acc"))
                input_df = input_df.join(cds_df, [cds_df.idx == biopfam_scan_df.cds_id]).select(
                    F.concat(
                        cds_df.idx.cast(T.StringType()), F.lit("|"), biopfam_scan_df.cds_from, F.lit("-"), biopfam_scan_df.cds_to
                    ).alias("name"),
                    F.expr("substr(aa_sequence, cds_from, cds_to)").alias("aa_sequence"),
                    F.col("subpfam_hmm_path")
                )

                # run subpfam scan and save to parquet
                result_df = input_df.rdd.flatMap(
                    lambda row: run_hmmscan(
                        [(row.name, row.aa_sequence)], row.subpfam_hmm_path,
                        bit_cutoff=20,
                        use_unoptimized_hmm=True
                    )
                ).toDF(T.StructType([
                    T.StructField("query_name", T.StringType()),
                    T.StructField("query_from", T.LongType()),
                    T.StructField("query_to", T.LongType()),
                    T.StructField("query_gaps", T.ArrayType(T.LongType())),
                    T.StructField("hmm_acc", T.StringType()),
                    T.StructField("hmm_name", T.StringType()),
                    T.StructField("hmm_from", T.LongType()),
                    T.StructField("hmm_to", T.LongType()),
                    T.StructField("hmm_gaps", T.ArrayType(T.LongType())),
                    T.StructField("bitscore", T.FloatType())
                ])).select("query_name", "hmm_name", "bitscore")\
                    .withColumn(
                        "corepfam", F.udf(lambda name: name.split(".aligned_c")[0], T.StringType())("hmm_name")
                    )\
                    .withColumn(
                        "subpfam", F.udf(lambda name: int(name.split(".aligned_c")[1]), T.LongType())("hmm_name")
                    )\
                    .select("query_name", "corepfam", "subpfam", "bitscore")\
                    .withColumn("rank", F.row_number().over(
                        W.partitionBy(["query_name", "corepfam"]).orderBy(F.col("bitscore").desc())
                    ))\
                .select("query_name", "corepfam", "subpfam").groupBy(["query_name", "corepfam"]).agg(
                    F.collect_list("subpfam").alias("sub_pfams")
                ).withColumn(
                    "cds_id", F.udf(lambda name: int(name.split("|")[0]), T.LongType())("query_name")
                ).withColumn(
                    "cds_from", F.udf(lambda name: int(name.split("|")[1].split("-")[0]), T.LongType())("query_name")
                ).withColumn(
                    "cds_to", F.udf(lambda name: int(name.split("|")[1].split("-")[1]), T.LongType())("query_name")
                )\
                .select("cds_id", "cds_from", "cds_to", "corepfam", "sub_pfams")

                result_df.write.parquet(subpfam_pq_path)
            else:
                print("fetching biopfam-scan result...")

            # first, get merged features dataframe of biopfam hits and subpfam hits
            cds_to_bgc_df = self.getData("cds_to_bgc").df
            biopfam_scan_df = spark.read.parquet(biopfam_pq_path)
            subpfam_scan_df = spark.read.parquet(subpfam_pq_path)

            # example result of this step:
            # --------------------------------------------------------
            # | bgc_id | biopfams                                    |
            # --------------------------------------------------------
            # | 0      | [["PKS_KS", "255"], ["PKS_AT", "255"] ...]] |
            # --------------------------------------------------------
            biopfam_feat_df = biopfam_scan_df.join(cds_to_bgc_df, [biopfam_scan_df.cds_id == cds_to_bgc_df.idx_1]).select(
                biopfam_scan_df.hmm_acc, cds_to_bgc_df.idx_2.alias("bgc_id")
            ).distinct().groupBy("bgc_id").agg(
                F.udf(
                    lambda biopfams: [[biopfam, 255] for biopfam in biopfams],
                    T.ArrayType(T.ArrayType(T.StringType()))
                )(F.collect_list("hmm_acc")).alias("biopfams")
            ).groupBy("bgc_id").agg(F.flatten(F.collect_list("biopfams")).alias("biopfams"))

            # example result of this step:
            # ------------------------------------------------------------
            # | bgc_id | subpfams                                        |
            # ------------------------------------------------------------
            # | 0      | [["PKS_KS:1", "255"], ["PKS_KS:5", "155"] ...]] |
            # ------------------------------------------------------------
            subpfam_feat_df = subpfam_scan_df.join(cds_to_bgc_df, [subpfam_scan_df.cds_id == cds_to_bgc_df.idx_1]).select(
                F.udf(
                    lambda corepfam, subpfams: [
                        ["{}:{}".format(corepfam, subpfam), (int(255 - int((255 / top_k) * i)))] for i, subpfam in enumerate(subpfams[:top_k])
                    ],
                    T.ArrayType(T.ArrayType(T.StringType()))
                )(F.col("corepfam"), F.col("sub_pfams")).alias("subpfams"), cds_to_bgc_df.idx_2.alias("bgc_id"), 
            ).groupBy("bgc_id").agg(F.flatten(F.collect_list("subpfams")).alias("subpfams"))
            
            def get_feature_vector(biopfams, subpfams):
                # merge all key-value pairs, retain max values for every unique keys
                merged_features = list(map(
                    lambda x: (x[0], int(x[1])),
                    (biopfams + subpfams if (biopfams is not None and subpfams is not None)
                        else (biopfams if biopfams else (subpfams if subpfams else [])))
                ))
                dict_feat = {}
                for (key, val) in merged_features:
                    dict_feat[key] = float(max(val, dict_feat.get(key, 0)))
                return dict_feat

            # merge both biopfam features and subpfam features, then store feature df
            # example result of this step:
            # ------------------------------------------------------
            # | bgc_id | features                                  |
            # ------------------------------------------------------
            # | 0      | { "PKS_KS:1": 255, "PKS_KS:5": 155, ... } |
            # ------------------------------------------------------
            print("calculating feature vectors... k={}".format(top_k))
            feature_df = biopfam_feat_df.join(subpfam_feat_df, "bgc_id", "outer").select(
                F.col("bgc_id"),
                F.udf(
                    get_feature_vector, T.MapType(T.StringType(), T.DoubleType())
                )(F.col("biopfams"), F.col("subpfams")).alias("features")
            )
            feature_df.write.parquet(feature_pq_path)
        else:
            print("fetching feature vectors... k={}".format(top_k))

        return spark.read.parquet(feature_pq_path)
