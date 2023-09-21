"""
Unit testing module for axolotl.data.base and other base classes directly under axolotl.data
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


def test_axl_df(spark):
	sc = spark.sparkContext
	
	print(">>> Testing AxlDF, ioDF and MetaDF <<<")

	from axolotl.data import MetaDF # using MetaDF to test AxlDF and ioDF too
	from axolotl.data import RelationDF
	import pyspark.sql.types as T
	import pyspark.sql.functions as F
	from axolotl.utils.file import get_temp_dir
	from os import path

	dummy_df = sc.parallelize([
		{ "key": "filename", "value": "1.txt", "file_path": "1.txt" },
		{ "key": "date", "value": "2023-08-01", "file_path": "1.txt" },
		{ "key": "filename", "value": "2.txt", "file_path": "2.txt" },
		{ "key": "date", "value": None, "file_path": "2.txt" }
	]).toDF(MetaDF._getSchema())
	dummy_df.cache()
	dummy_df.collect()

	print(">>> Testing class instantiation and validation methods...")
	assert MetaDF(dummy_df).countValids() == (3, 1)

	print(">>> Testing read() and write()...")
	with get_temp_dir() as tmp_dir:
		MetaDF(dummy_df).write(path.join(tmp_dir, "stored_df"))
		loaded_df = MetaDF.read(path.join(tmp_dir, "stored_df"))
		assert loaded_df.countValids() == (3, 1)

	print(">>> Testing RelationDF <<<")
	axldf_1 = MetaDF(dummy_df)
	axldf_2 = MetaDF(dummy_df)
	join_df = axldf_1.df.join(axldf_2.df, "file_path").select(
		axldf_1.df["idx"].alias("idx_1"),
		axldf_2.df["idx"].alias("idx_2")
	)
	assert RelationDF(join_df).countValids() == (8, 0)