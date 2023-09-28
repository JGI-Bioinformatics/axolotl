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
from axolotl.app.bgc_analysis.functions import scan_bigslice_db_folder, scan_cdsDF, run_hmmscan

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

    def _creationFunc(self, sequences: NuclSeqDF, features: RawFeatDF, source_type: str="antismash"):
        """
        extract bgc_df, cds_df and build cds_to_bgc dataframe that will act as BigsliceApp's core data
        """

        # first, extract bgcDF from features
        bgc_df = bgcDF.fromRawFeatDF(features, sequences, source_type = source_type)
        self._setData("bgc", bgc_df)
        self._saveData("bgc")
        bgc_df = self._getData("bgc")  

        # then, extract unfiltered cdsDF from features (might be genomic)
        cds_df = cdsDF.fromRawFeatDF(features, sequences)

        # perform join to match only cds in bgcs
        # idx_1 == cds, idx_2 == bgc
        cds_bgc_df = RelationDF(bgc_df.df.join(cds_df.df, [
            bgc_df.df.file_path == cds_df.df.file_path,
            bgc_df.df.source_path == cds_df.df.source_path,
            bgc_df.df.seq_id == cds_df.df.seq_id,
            bgc_df.df.location.start <= cds_df.df.location.start,
            bgc_df.df.location.end >= cds_df.df.location.end
        ]).select(
            F.when(F.lit(True), cds_df.df.idx).alias("idx_1"),
            F.when(F.lit(True), bgc_df.df.idx).alias("idx_2")
        ))
        self._setData("cds_to_bgc", cds_bgc_df)
        self._saveData("cds_to_bgc")
        cds_bgc_df = self._getData("cds_to_bgc")

        # filter only cds in bgcs, then save cdsDF
        cds_df.df = cds_df.df.join(
            cds_bgc_df.df.select("idx_1").distinct(),
            [ cds_df.df.idx == cds_bgc_df.df.idx_1 ]
        ).select(cds_df.df.columns)
        self._setData("cds", cds_df)
        self._saveData("cds")

        # now that cdsDF is filtered and stored properly,
        # update only the metadata of cds_to_bgc DF
        cds_bgc_df.updateSources([cds_df, bgc_df])
        self._saveData("cds_to_bgc", overwrite=True)

    def _loadExtraData(self):
        return

    ### BigsliceApp-specific functions ###

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

            # check if already subpfam-scanned
            subpfam_pq_path = path.join(feature_folder, "subpfam-{}-{}".format(
                biopfam_md5, subpfam_md5
            ))
            if not check_file_exists(subpfam_pq_path):
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
                ).toDF().select("query_name", "hmm_name", "bitscore")\
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
                    dict_feat[key] = max(val, dict_feat.get(key, 0))                
                return dict_feat

            # merge both biopfam features and subpfam features, then store feature df
            # example result of this step:
            # ------------------------------------------------------
            # | bgc_id | features                                  |
            # ------------------------------------------------------
            # | 0      | { "PKS_KS:1": 255, "PKS_KS:5": 155, ... } |
            # ------------------------------------------------------
            feature_df = biopfam_feat_df.join(subpfam_feat_df, "bgc_id", "outer").select(
                F.col("bgc_id"),
                F.udf(
                    get_feature_vector, T.MapType(T.StringType(), T.IntegerType())
                )(F.col("biopfams"), F.col("subpfams")).alias("features")
            )
            feature_df.write.parquet(feature_pq_path)

        return spark.read.parquet(feature_pq_path)