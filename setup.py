#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

from builtins import *
from shutil import copytree, copy, rmtree
import os
import sys
from distutils.command.build_ext import build_ext
import glob
from setuptools.command.install import install

def remove_if_exists(file_path):
    if os.path.exists(file_path):
        if os.path.islink(file_path) or os.path.isfile(file_path):
            os.remove(file_path)
        else:
            assert os.path.isdir(file_path)
            rmtree(file_path)


def find_file_path(pattern):
    files = glob.glob(pattern)
    if len(files) < 1:
        print("Failed to find the file %s." % pattern)
        exit(-1)
    if len(files) > 1:
        print("The file pattern %s is ambiguous: %s" % (pattern, files))
        exit(-1)
    return files[0]


current_dir = os.path.abspath(os.path.dirname(__file__))

JAR_PATH = "jars"

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'umap-learn',
    'pandas',
    'numpy', 
]

test_requirements = ['pytest>=3', ]

PACKAGE_DIR = {"axolotl.jars" : JAR_PATH}
PACKAGE_DATA = {"axolotl.jars" : ["*.jar"]}

setup(
    author="Zhong Wang",
    author_email='zhongwang@lbl.gov',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="A Python PGenomics Library based on pySpark",
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='axolotl',
    name='axolotl',
    packages=find_packages(include=['axolotl', 'axolotl.*', "axolotl.jars"]),
    package_dir=PACKAGE_DIR,
    package_data=PACKAGE_DATA,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/zhongwang/axolotl',
    version='0.1.1',
    zip_safe=False,
)
