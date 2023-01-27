#!/usr/bin/env python

"""Tests for `axolotl` aligner function."""

import pytest
import yaml
import tests.test_spark_env

from axolotl.aligner.aligner import seq_align

def read_yaml(file_path): 
    """read config file"""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

@pytest.mark.usefixtures("spark_session")
def test_aligner(spark_session):
    """Testing aligner function."""
    my_testing = read_yaml('../config.yml')
    for format, path in my_testing['reads']:
        reads = spark_session.read.parquet(path)
        for aligner, command in my_testing['aligners']:
            aligned = seq_align(reads, command, format=format).count()
            assert aligned >0, f"Aligner:{aligner} on {format} format produced number of alignments greater than 0 expected, got: {aligned}"


