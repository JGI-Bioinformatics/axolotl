"""
Unit testing module for axolotl.app
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


def test_axl_app(spark):
    sc = spark.sparkContext

    from axolotl.app import ExampleApp

    from axolotl.utils.file import get_temp_dir, fopen
    import pyspark.sql.types as T
    import pyspark.sql.functions as F
    from os import path
    import glob

    with get_temp_dir() as tmp_dir:

        path_app_storage = path.join(tmp_dir, "example_app")
        path_input_folder = path.join(path.dirname(path.dirname(path.abspath(__file__))), "io", "genbank", "example_gbk_files")

        print(">>> Testing creation of new exampleApp and basic function calling... <<<")
        exp_app = ExampleApp.getOrCreate(path_app_storage, genbanks_path = path.join(path_input_folder, "*.gbk"))
        #assert sum([row["count"] for row in exp_app.getCDScount().rdd.collect()]) == 129

        print(">>> Testing loading of previous exampleApp and extradata calling... <<<")
        exp_app_2 = ExampleApp.getOrCreate(path_app_storage)
        assert exp_app_2._dummy_metadata == "just_a_string"