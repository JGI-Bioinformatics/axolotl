"""
Unit testing module for axolotl.data.sequence
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


def test_axl_data_sequence(spark):
	sc = spark.sparkContext

	print(">>> Testing NuclSeqDF <<<")
	from axolotl.data.sequence import NuclSeqDF
	dummy_df = sc.parallelize([
		{ "file_path": "1.fa", "seq_id": "ctg_1", "desc": "contig 1", "sequence": "ATGCatgcNn", "length": 10 },
		{ "file_path": "1.fa", "seq_id": "ctg_2", "desc": "contig 2", "sequence": "ATGCatgc", "length": 10 },
		{ "file_path": "2.fa", "seq_id": "ctg_1", "desc": "contig 1", "sequence": "ATGCatgcXXNn", "length": 12 },
		{ "file_path": "2.fa", "seq_id": "ctg_2", "desc": "contig 2", "sequence": "", "length": 0 },
	]).toDF(NuclSeqDF._getSchema())
	assert NuclSeqDF(dummy_df).countValids() == (1, 3)

	print(">>> Testing ReadSeqDF <<<")
	from axolotl.data.sequence import ReadSeqDF
	dummy_df = sc.parallelize([
		{ "file_path": "1.fq", "seq_id": "read_1", "desc": "contig 1", "sequence": "ATGCatgcNn",
			"quality": [100]*10, "length": 10 },
		{ "file_path": "1.fq", "seq_id": "read_2", "desc": "contig 1", "sequence": "ATGCatgcNn",
			"quality": [100]*9, "length": 10 },
		{ "file_path": "1.fq", "seq_id": "read_3", "desc": "contig 1", "sequence": "ATGCatgcNnXx",
			"quality": [100]*12, "length": 12 },
	]).toDF(ReadSeqDF._getSchema())
	assert ReadSeqDF(dummy_df).countValids() == (1, 2)

	print(">>> Testing PReadSeqDF <<<")
	from axolotl.data.sequence import PReadSeqDF
	dummy_df = sc.parallelize([
		{ "file_path": "1.fq", "seq_id": "read_1", "desc": "contig 1", "sequence": "ATGCatgcNn",
			"quality": [100]*10, "length": 10, "sequence_2": "ATGCatgcNn", "quality_2": [100]*10, "length_2": 10 },
		{ "file_path": "1.fq", "seq_id": "read_2", "desc": "contig 1", "sequence": "ATGCatgcNn",
			"quality": [100]*9, "length": 10, "sequence_2": "ATGCatgcNn", "quality_2": [100]*10, "length_2": 10 },
		{ "file_path": "1.fq", "seq_id": "read_3", "desc": "contig 1", "sequence": "ATGCatgcNnXx",
			"quality": [100]*12, "length": 10, "sequence_2": "ATGCatgcNn", "quality_2": [100]*10, "length_2": 12 },
	]).toDF(PReadSeqDF._getSchema())
	assert PReadSeqDF(dummy_df).countValids() == (1, 2)

	print(">>> Testing ProtSeqDF <<<")
	from axolotl.data.sequence import ProtSeqDF
	dummy_df = sc.parallelize([
		{ "file_path": "1.fa", "seq_id": "prot_1", "desc": "protein 1", "sequence": "ATGCatgcNn", "length": 10 },
		{ "file_path": "1.fa", "seq_id": "prot_2", "desc": "protein 2", "sequence": "ABCDEFGHIJKLMNOPQRSTUVWYZX*-abcdefghijklmnopqrstuvwyzx", "length": 10 },
		{ "file_path": "1.fa", "seq_id": "prot_3", "desc": "protein 3", "sequence": "-123112", "length": 10 },
		{ "file_path": "1.fa", "seq_id": "prot_4", "desc": "protein 4", "sequence": "", "length": 0 },
	]).toDF(ProtSeqDF._getSchema())
	assert ProtSeqDF(dummy_df).countValids() == (1, 3)