"""
Unit testing module for axolotl.io.fasta
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


def test_axl_io_fasta(spark):
    sc = spark.sparkContext

    from axolotl.io.fasta import FastaIO

    from axolotl.utils.file import get_temp_dir, fopen
    import pyspark.sql.types as T
    from os import path
    import glob

    with get_temp_dir() as tmp_dir:

        # put dummy input files
        for i in range(1, 5):
            with fopen(path.join(tmp_dir, "{}.fna".format(i)), "w") as input_stream:
                input_stream.write("\n".join([
                    ">ctg_1\nATGCATGCATGCATGCATGCATGCATGCATGCATGC",
                    ">ctg_2\nMAMAMAMAMAMAMAMAMAMAMAMAMAMAMAMAMAMA",
                    ">ctg_3\nATGCATGCATGCATGCATGCATGCATGCATGCATGC",
                    ">ctg_4\nMAMAMAMAMAMAMAMAMAMAMAMAMAMAMAMAMAMA",
                ]))

        print(">>> Testing nucleotide file & loadSmallFiles <<<")
        assert (FastaIO.loadSmallFiles(path.join(tmp_dir, "*.fna"), seq_type="nucl").countValids() == (8, 8))

        print(">>> Testing protein file & loadBigFiles <<<")
        assert (FastaIO.loadBigFiles(glob.glob(path.join(tmp_dir, "*.fna")), path.join(tmp_dir, "tmp_bigfile"), seq_type="prot").countValids() == (16, 0))
