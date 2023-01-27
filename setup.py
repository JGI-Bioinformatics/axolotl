import setuptools

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

setuptools.setup(
    name="axolotl",
    version="0.2.0",
    scripts=[
    ],
    author="Joint Genome Institute - Genome Analysis R&D",
    author_email="zhongwang@lbl.gov",
    description=("A Python PGenomics Library based on pySpark"),
    long_description=readme + '\n\n' + history,
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
    ],
    python_requires=">=3.6",
    test_suite="tests",
    install_requires=[
        "pyspark"
    ],
    tests_require=[
    ],
    license="BSD license",
    zip_safe=False
)