"""
Unit testing module for axolotl.io.base
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


def test_axl_io(spark):
    sc = spark.sparkContext
    
    print(">>> Testing AxlIO via dummyIO <<<")

    from axolotl.io import DummyIO
    from axolotl.data import MetaDF
    import pyspark.sql.types as T
    from axolotl.utils.file import get_temp_dir, fopen
    from os import path

    with get_temp_dir() as tmp_dir:

        # put dummy input files
        with fopen(path.join(tmp_dir, "dummy_file_1.txt"), "w") as input_stream:
            input_stream.write("\n".join([
                "key_1,value_1",
                "key_2,value_2",
                "key_3,value_3",
                "key_4,value_4"
            ]))
        with fopen(path.join(tmp_dir, "dummy_file_2.txt"), "w") as input_stream:
            input_stream.write("\n".join([
                "key_1,value_21",
                "key_2,value_22",
                "key_3,value_23",
                "key_4,value_24"
            ]))

        print(">>> Testing loadSmallFiles() <<<")
        parsed_meta_df = DummyIO.loadSmallFiles(
            path.join(tmp_dir, "*.txt"),
            params = { "prefix": "params-checked-" }
        )
        assert parsed_meta_df.df.filter("key like 'params-checked-%'").count() == 8

        print(">>> Testing loadBigFiles() <<<")
        parsed_meta_df = DummyIO.loadBigFiles(
            [path.join(tmp_dir, "dummy_file_1.txt"), path.join(tmp_dir, "dummy_file_1.txt")],
            intermediate_pq_path = path.join(tmp_dir, "intermediate_pq_path"),
            params = { "prefix": "params-checked-" }
        )
        assert parsed_meta_df.df.filter("key like 'params-checked-preprocessed-params-checked-%'").count() == 8

        print(">>> Testing concatSmallFiles() and loadConcatenatedFiles() <<<")
        DummyIO.concatSmallFiles(
            path.join(tmp_dir, "*.txt"),
            path.join(tmp_dir, "concatenated_files.txt")
        )
        parsed_meta_df = DummyIO.loadConcatenatedFiles(
            path.join(tmp_dir, "concatenated_files.txt"),
            params = { "prefix": "params-checked-" }
        )
        assert parsed_meta_df.df.filter("key like 'params-checked-%'").count() == 8

