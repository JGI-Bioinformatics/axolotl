#!/usr/bin/env python

"""Tests for `axolotl` aligner function."""

import pytest


from axolotl.aligner.aligner import seq_align
from pyspark.sql import SparkSession
spark = SparkSession.getOrCreate()

@pytest.fixture
def short_reads():
    """parquet-format reads converted from fasta and fastq format"""
    reads_seq = {
        'fa' : '',
        'fq' : ''
    }
    return reads_seq

@pytest.fixture
def aligners():
    """aligner strings"""
    aligners = {
        'minmap2' : '',
        'bbmap' : ''
    }
    return aligners

def test_content(short_reads, aligners):
    """Testing aligner function."""
    for format, path in short_reads.items():
        reads = spark.read.parquet(path)
        for aligner, command in aligners.items():
            try:
                seq_align(reads, command, format=format)
                print('Testing aligner "%s" on %s format... Passed' % (aligner, format))
            except:
                print('Testing aligner "%s" on %s format... Failed' % (aligner, format))
