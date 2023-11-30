import setuptools

setuptools.setup(
    name="axolotl",
    version="0.2.0",
    scripts=[
        "axolotl/axolotl"
    ],
    author="Joint Genome Institute - Genome Analysis R&D",
    author_email="zhongwang@lbl.gov",
    description=("A Python Genomics Library based on pySpark"),
    long_description="",
    url="https://github.com/zhongwang/axolotl",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.11',
    ],
    python_requires=">=3.6",
    test_suite="tests",
    install_requires=[
        "pyspark",
        "pyarrow",
        "biopython<=1.77",
        "pyhmmer",
        "scikit-learn",
        "numpy",
        "pandas",
        "tqdm"
    ],
    tests_require=[
    ],
    license="BSD license",
    zip_safe=False
)