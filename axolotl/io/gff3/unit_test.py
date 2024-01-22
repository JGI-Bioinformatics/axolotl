"""
Unit testing module for axolotl.io.gff3
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


def test_axl_io_gff3(spark):
    sc = spark.sparkContext

    from axolotl.io.gff3 import gff3IO

    from axolotl.utils.file import get_temp_dir, fopen
    import pyspark.sql.types as T
    from os import path
    import glob

    with get_temp_dir() as tmp_dir:

        path_input_folder = path.join(path.dirname(path.abspath(__file__)), "example_gff3_files")

        print(">>> Testing extracting features with loadSmallFiles <<<")
        assert gff3IO.loadSmallFiles(path.join(path_input_folder, "*.gff")).df.count() == 32085