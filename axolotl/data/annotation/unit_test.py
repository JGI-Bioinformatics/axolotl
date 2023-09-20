"""
Unit testing module for axolotl.data.annotation
"""

#####################################################################################

import logging

import pytest
from pyspark.sql import SparkSession

def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark(request):
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .master('local[*]')
             .appName('pytest-pyspark-local-testing')
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark

#####################################################################################


def test_axl_data_annotation_feature(spark):
    sc = spark.sparkContext

    print(">>> Testing RawFeatDF <<<")
    from axolotl.data.annotation import RawFeatDF
    dummy_df = sc.parallelize([
        {
            "file_path": "1.gff", "source_path": "1.fa", "seq_id": "contig_1", "type": "CDS",
            "location": [
            ],
            "qualifiers": [{"key": "locus_tag", "values": ["CDS001"]}]
        },
        {
            "file_path": "1.gff", "source_path": "1.fa", "seq_id": "contig_1", "type": "CDS",
            "location": [
                { "start": 1, "end": 150, "strand": 1 }
            ],
            "qualifiers": [{"key": "locus_tag", "values": ["CDS002"]}]
        },
        {
            "file_path": "1.gff", "source_path": "1.fa", "seq_id": "contig_1", "type": "gene",
            "location": [
                { "start": 1, "end": 150, "strand": 1 }
            ],
            "qualifiers": []
        }
    ]).toDF(RawFeatDF._getSchema())

    print(">>> Testing class instantiation and validation methods...")
    assert RawFeatDF(dummy_df).countValids() == (2, 1)\

    print(">>> Testing getSimpleLocation() function...")
    assert RawFeatDF.getSimpleLocation([
        { "start": 300, "end": 350, "strand": -1 },
        { "start": 200, "end": 250, "strand": -1 },
        { "start": 1, "end": 150, "strand": -1 }
    ]) == { "start": 1, "end": 350, "strand": -1}


def test_axl_data_annotation_cds(spark):
    sc = spark.sparkContext

    print(">>> Testing cdsDF <<<")
    from axolotl.data.sequence import NuclSeqDF
    from axolotl.data.annotation import RawFeatDF, cdsDF

    dummy_seq_df = NuclSeqDF(sc.parallelize([
        { "file_path": "1.fa", "seq_id": "ctg_1", "desc": "contig 1", "sequence": "ATGC"*1000, "length": 4000 }
    ]).toDF(NuclSeqDF._getSchema()))

    dummy_feat_df = RawFeatDF(sc.parallelize([
        {
            "file_path": "1.gff", "source_path": "1.fa", "seq_id": "ctg_1", "type": "CDS",
            "location": [
                { "start": 1, "end": 12, "strand": -1 }
            ],
            "qualifiers": [
                {"key": "locus_tag", "values": ["CDS001"]},
                {"key": "transl_table", "values": ["11"]}
            ]
        }
    ]).toDF(RawFeatDF._getSchema()))

    print(">>> Testing class instantiation and translation function...")
    assert cdsDF.fromRawFeatDF(dummy_feat_df, dummy_seq_df).df.rdd.take(1)[0]["aa_sequence"] == "ACMH"


def test_axl_data_annotation_bgc(spark):
    sc = spark.sparkContext

    print(">>> Testing bgcDF <<<")
    from axolotl.data.sequence import NuclSeqDF
    from axolotl.data.annotation import RawFeatDF, bgcDF

    dummy_seq_df = NuclSeqDF(sc.parallelize([
        { "file_path": "1.fa", "seq_id": "ctg_1", "desc": "contig 1", "sequence": "ATGC"*1000, "length": 4000 }
    ]).toDF(NuclSeqDF._getSchema()))

    dummy_feat_df = RawFeatDF(sc.parallelize([
        {
            "file_path": "1.gff", "source_path": "1.fa", "seq_id": "ctg_1", "type": "region",
            "location": [
                { "start": 1, "end": 12, "strand": 1 }
            ],
            "qualifiers": [
                {"key": "contig_edge", "values": ["False"]},
                {"key": "product", "values": ["PKS", "NRPS"]}
            ]
        },
        {
            "file_path": "1.gff", "source_path": "1.fa", "seq_id": "ctg_1", "type": "cluster",
            "location": [
                { "start": 3950, "end": 4000, "strand": 1 }
            ],
            "qualifiers": [
                {"key": "BGC_Class", "values": ["smc/PKS", "smc/NRPS"]}
            ]
        },
        {
            "file_path": "1.gff", "source_path": "1.fa", "seq_id": "ctg_1", "type": "cluster",
            "location": [
                { "start": 1, "end": 12, "strand": 1 }
            ],
            "qualifiers": [
                {"key": "random_thing", "values": ["random"]}
            ]
        }
    ]).toDF(RawFeatDF._getSchema()))

    print(">>> Testing extracting BGCs from antiSMASH rows...")
    assert bgcDF.fromRawFeatDF(dummy_feat_df, dummy_seq_df, source_type="antismash").df.collect()[0]["classes"] == ["PKS", "NRPS"]

    print(">>> Testing extracting BGCs from SMC rows...")
    assert bgcDF.fromRawFeatDF(dummy_feat_df, dummy_seq_df, source_type="smc").df.collect()[-1]["on_contig_edge"]