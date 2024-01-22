"""
Unit testing module for axolotl.io.genbank
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


def test_axl_io_genbank(spark):
    sc = spark.sparkContext

    from axolotl.io.genbank import gbkIO

    from axolotl.utils.file import get_temp_dir, fopen
    import pyspark.sql.types as T
    from os import path
    import glob

    with get_temp_dir() as tmp_dir:

        path_input_folder = path.join(path.dirname(path.abspath(__file__)), "example_gbk_files")

        print(">>> Testing extracting sequences with loadSmallFiles <<<")
        assert (gbkIO.loadSmallFiles(path.join(path_input_folder, "*.gbk"), df_type="sequence").countValids() == (4, 0))

        print(">>> Testing extracting features with loadBigFiles <<<")
        assert (
            gbkIO.loadBigFiles(
                glob.glob(path.join(path_input_folder, "*.gbk")),
                path.join(tmp_dir, "tmp_bigfile"),
                df_type="annotation"
            ).countValids() == (758, 0)
        )

        print(">>> Testing extracting metadata with loadConcatenatedFiles() <<<")
        gbkIO.concatSmallFiles(
            path.join(path_input_folder, "*.gbk"),
            path.join(tmp_dir, "concatenated_files.txt")
        )
        parsed_meta_df = gbkIO.loadConcatenatedFiles(
            path.join(tmp_dir, "concatenated_files.txt"),
                df_type="metadata"
        )
        assert parsed_meta_df.countValids() == (60, 0)
