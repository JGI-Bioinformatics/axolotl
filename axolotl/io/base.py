from abc import ABC, abstractmethod
from os import path
from typing import List, Dict, Tuple

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from axolotl.utils.file import check_file_exists, is_directory, parse_path_type, get_temp_dir
from axolotl.utils.spark import get_spark_session_and_context

from axolotl.data import ioDF


class AxlIO(ABC):
    """
    Base Axolotl class for handling the parallel parsing of raw input files commonly used
    in omics analyses (e.g., FASTA/Q, GenBank, GFF3, VCF, SAM, etc.). All AxlIO modules will
    take a specific raw input format, then outputs an ioDF (AxlDF with a 'file_path' column)
    object representing the information contained in the raw input now encoded in a tabular
    format (i.e., DataFrame) for further processing.

    The recommended way to use any AxlIO function is to directly store its ioDF output, then
    later on (or subsequently) instantiate the same ioDF class from the stored parquet output.
    See this example:

        from axolotl.io import NuclFastaIO
        from axolotl.data import NuclSeqDF

        # parse input fasta files and store the resulting AxlDF object
        NuclFastaIO.loadSmallFiles("/home/all_genome_fastas/*.fa").write("/home/all_genome_fastas_axldf")

        # load previously-processed input files
        sequences_df = NuclSeqDF.read("/home/all_genome_fastas_axldf")

        # show dataframe
        sequences_df.df.show()

    For all AxlIO sub-classes, there are three ways to load input files, depending on the nature
    of the files itself:

    1) AxlIO.loadSmallFiles() --> use this when dealing with many small files as input (e.g., 100,000 bacterial
       genome files). This mode will use Spark's native "wholeTextFiles" to resolve the input files pattern
       (use '*' for wildcard) and _immediately_ distribute the partition based on a specific record separator.
       In this case, all file contents will be treated like it was coming from a giant concatenated file.
       One limitation of this mode is that it will only support textual file inputs (no zip files!) and that it
       doesn't allow pre-processing, therefore users should perform any pre-processing themselves before using
       Axolotl.

    2) AxlIO.loadBigFiles() --> use this when dealing with multiple big files (e.g., >1GB VCF files) or when you
       need to apply some pre-processing before the files can be read by Spark (e.g., zipped FASTQ files). Different
       from mode #1, Axolotl will take a list of every filepaths and work on each big file in series, i.e.,:
          -> input = [file_path#1, file_path#2, file_path#3]
          -> read file_path#1 -> perform preprocessing on file_path#1 -> parse file_path#1 using Spark -> store intermediate pq
          -> read file_path#2 -> perform preprocessing on file_path#2 -> parse file_path#2 using Spark -> store intermediate pq
          -> read file_path#3 -> perform preprocessing on file_path#3 -> parse file_path#3 using Spark -> store intermediate pq
          -> concat all intermediate_pq files and store as a final ioDF parquet
       do note, however, that since all pre-processing will happen on the driver node, you need to make sure that the
       driver node is powerful enough to handle the operation successfully.

    3) AxlIO.loadConcatenatedFiles() --> work in concordance with AxlIO.concatSmallFiles(), this mode was meant
       for users working with many small files (as with mode #1) but are using distributed data storage such as
       Hadoop FileSystem (HDFS). Since storing many small files will introduce a lot overhead in such storage
       system, users can use this mode to pre-process and pack the files into chunks of bigger files that are
       more appropriately-sized for HDFS. Axolotl will seamlessly tracks all original filenames of the small files
       and correctly incorporate them into the resulting ioDF object.

    -----------------

    Subclassing an AxlIO class: implements all methods under "##### TO BE IMPLEMENTED BY SUBCLASSES #####". You can also
    see an example in the axolotl.io.dummyio module.

    """
    
    @classmethod
    def loadSmallFiles(cls, file_pattern: str, *args, **kwargs) -> ioDF:
        """
        take an input file pattern (in "glob"-style) and parse every identifiable files into
        an ioDF object. Always make sure that the input files are texts (i.e., unzipped) since
        this method will only be able to read text files.
        Use params to pass any subclass-specific parameters into your parsing.
        """

        spark, sc = get_spark_session_and_context()
        
        # input check
        if not isinstance(file_pattern, str):
            raise TypeError("expected file_pattern to be a string")
            
        return cls.__postprocess(cls._getOutputDFclass(*args, **kwargs)(
            sc.wholeTextFiles(file_pattern)\
                .flatMap(lambda x: cls._parseFile(x[0], x[1], *args, **kwargs))\
                .toDF(schema = cls._getOutputDFclass(*args, **kwargs)._getSchema())
        ), *args, **kwargs)

    @classmethod
    def concatSmallFiles(cls, file_pattern: str, path_output: str, num_partitions: int=-1):
        """
        take an input file pattern (in "glob"-style) and concatenate them into chunks of
        bigger files (defined by 'num_partitions') to allow efficient storage in distributed
        system such as HDFS.
        """

        spark, sc = get_spark_session_and_context()
        
        # make sure the outputh path is empty
        if check_file_exists(path_output):
            raise Exception("path_output file path exists! {}".format(path_output))
        if not isinstance(file_pattern, str):
            raise TypeError("expected file_pattern to be a string")
        
        # import RDD
        rdd_imported = sc.wholeTextFiles(file_pattern)\
        .reduceByKey(lambda row1, row2: row1)\
        .map(lambda x: cls._getFileDelimiter()[0] + x[0] + cls._getFileDelimiter()[1] + x[1])\
        
        if num_partitions > 0:
            rdd_imported = rdd_imported.repartition(num_partitions)            
        
        rdd_imported.saveAsTextFile(path_output)

    @classmethod
    def loadConcatenatedFiles(cls, file_pattern: str, persist: bool=True, intermediate_pq_path: str="", *args, **kwargs) -> ioDF:
        """
        load the previously-concatenated small files (see concatSmallFiles()) and parse them
        in parallel using Spark.
        """

        spark, sc = get_spark_session_and_context()
        
        # input check
        if not isinstance(file_pattern, str):
            raise TypeError("expected file_pattern to be a string")
        if not isinstance(intermediate_pq_path, str):
            raise TypeError("expected intermediate_pq_path to be a string")
        if not isinstance(persist, bool):
            raise TypeError("expected persist to be a boolean")
            
        if intermediate_pq_path != "":
            if check_file_exists(intermediate_pq_path):
                raise Exception("intermediate parque file path exists! {}".format(intermediate_pq_path))
        else:
            if not persist:
                raise Exception("either persist needs to be True or intermediate_pq_path needs to be supplied")

        # change delimiter for the custom textFiles() function
        delim_default = sc._jsc.hadoopConfiguration().get("textinputformat.record.delimiter")
        sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", cls._getFileDelimiter()[0])

        # parse dataframe and evaluate
        df = sc.textFile(file_pattern)\
            .map(lambda x: x[:-1])\
            .filter(lambda x: x != "")\
            .map(lambda x: tuple(x.split(cls._getFileDelimiter()[1], 1)))\
            .flatMap(lambda x: cls._parseFile(x[0], x[1], *args, **kwargs))\
            .toDF(schema = cls._getOutputDFclass(*args, **kwargs)._getSchema())

        if intermediate_pq_path != "":
            df.write.parquet(intermediate_pq_path)
            df = spark.read.parquet(intermediate_pq_path)
        elif persist:
            df.persist()
            df.count()
        else:
            raise Exception("either persist needs to be True or intermediate_pq_path needs to be supplied")

        # revert delimiter back to what it was before
        if delim_default != None:
            sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", delim_default)
        else:
            sc._jsc.hadoopConfiguration().unset("textinputformat.record.delimiter")
            
        return cls.__postprocess(cls._getOutputDFclass(*args, **kwargs)(df), *args, **kwargs)
        
    @classmethod
    def loadBigFiles(cls, file_paths: List[str], intermediate_pq_path: str, skip_malformed_record: bool=False, *args, **kwargs) -> ioDF:
        """
        take a list of filepaths representing "big" files, iterate through all the files and parse each
        using Spark in a subsequent manner. This method allows the use of _prepInput() for pre-processing
        the files before parsing. "intermediate_pq_path" will always be needed for this method for storing
        the intermedate parquet results of each file before later on combining them into one giant dataframe.
        """

        spark, sc = get_spark_session_and_context()
        
        # input check
        if not isinstance(intermediate_pq_path, str):
            raise TypeError("expected intermediate_pq_path to be a string")

        # check if previously processed
        use_preprocessed = False
        if check_file_exists(intermediate_pq_path):
            print("INFO: trying to load pre-processed intermediate_pq_path...")
            parsed_filepaths = set([
                row.file_path for row in\
                spark.read.parquet(intermediate_pq_path)\
                .select("file_path").distinct().collect()
            ])
            if file_paths == parsed_filepaths:
                use_preprocessed = True
            else:
                raise Exception((
                    "pre-processed intermediate_pq_path doesn't match"
                    " the list of input paths! {}"
                ).format(intermediate_pq_path))

        if not use_preprocessed:

            # check each files
            for file_path in file_paths:
                if not check_file_exists(file_path):
                    raise FileNotFoundError(file_path)
                elif is_directory(file_path):
                    raise FileNotFoundError("{} is a directory".format(file_path))
            
            # parse each file_path separately and store in the intermediate parquet storage
            for file_path in file_paths:
                print("INFO: parsing big file {}...".format(file_path))
                
                with get_temp_dir() as tmp_dir:

                    use_dbfs = False
                    if tmp_dir.startswith("dbfs:/"):
                        tmp_dir = tmp_dir.replace("dbfs:/", "/dbfs/", 1)
                        use_dbfs = True

                    # run preprocessing if necessary
                    text_file_path = cls._prepInput(file_path, tmp_dir, *args, **kwargs)
                    if use_dbfs:
                        text_file_path = text_file_path.replace("/dbfs/", "dbfs:/", 1)
                    
                    # change delimiter for the custom textFiles() function
                    delim_default = sc._jsc.hadoopConfiguration().get("textinputformat.record.delimiter")
                    sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", cls._getRecordDelimiter(*args, **kwargs))
                    # parse
                    _df = spark.createDataFrame(
                        sc.textFile(text_file_path).filter(lambda x: x != "").map(
                            lambda y: cls._parseRecord(y, *args, **kwargs)
                        ).flatMap(lambda n: n).filter(lambda z: (z != None) if skip_malformed_record else True),
                        schema = cls._getOutputDFclass(*args, **kwargs)._getSchemaSpecific()
                    )
                    _orig_cols = _df.columns
                    _df.withColumn("file_path", F.when(F.lit(True), F.lit(parse_path_type(file_path)["path"])))\
                        .select(["file_path"] + _orig_cols)\
                    .write.mode('append').parquet(intermediate_pq_path)            
                    del _df
                    del _orig_cols

                    # revert delimiter back to what it was before
                    if delim_default != None:
                        sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", delim_default)
                    else:
                        sc._jsc.hadoopConfiguration().unset("textinputformat.record.delimiter")
            
        # load DF from the intermediate parquet path, then output AxlDF
        return cls.__postprocess(cls._getOutputDFclass(*args, **kwargs)(spark.read.parquet(intermediate_pq_path)), *args, **kwargs)

    @classmethod
    def _getFileDelimiter(cls) -> Tuple[str, str]:
        """
        this method is used by the concatSmallFiles() and loadConcatenatedFiles() to keep track of the original
        small file's names. Generally, you don't need to override or modify this method.
        """

        return ("\n>>>>>file_path:", "\n")
    
    @classmethod
    def _parseFile(cls, file_path: str, text: str, skip_malformed_record: bool=False, *args, **kwargs) -> List[Row]:
        """
        this method is used by loadSmallFiles() and loadConcatenatedFiles() to break text files parsed by Spark
        into list of records based on the subclass-specific delimiters (defined via _getRecordDelimiter(*args, **kwargs)). Do not
        override this method unless you know what you are doing.
        """

        result = []
        for record_text in text.split(cls._getRecordDelimiter(*args, **kwargs)):
            if record_text == "":
                continue
            parsed_array = cls._parseRecord(record_text, *args, **kwargs)
            for parsed in parsed_array:
                if parsed == None and skip_malformed_record:
                    continue
                if isinstance(parsed, list):
                    result.append([parse_path_type(file_path)["path"]] + parsed)
                elif isinstance(parsed, dict):
                    result.append({ # the ** is to concatenate the two dictionaries
                        **{
                            "file_path": file_path
                        },
                        **parsed
                    })
                else:
                    raise NotImplementedError("_parseFile needs to return either lists of list or lists of dict")
        return result

    @classmethod
    def __postprocess(cls, data: ioDF, *args, **kwargs):
        """
        this method injects an AxlIO-wide general postprocessing step before calling subclass-specific
        postprocess (via _postprocess()) that works on the ioDF object of the parsed results.
        """

        data = cls._postprocess(data, *args, **kwargs)
        return data

    ##### TO BE IMPLEMENTED BY SUBCLASSES #####

    @classmethod
    @abstractmethod
    def _getRecordDelimiter(cls, *args, **kwargs) -> str:
        """
        mandatory override: specify the set of characters (i.e., a string) that serves as a "row-delimiter"
        in your input files, make sure to also include the newline in the delimiter. For example, in FASTA,
        you can use "\\n>" as the delimiter. See examples in the existing axolotl.io modules.

        Use the same *args and **kwargs throughout all _getRecordDelimiter(), _getOutputDFclass() and _parseRecord()
        """
        
        raise NotImplementedError("calling an unimplemented abstract method _getRecordDelimiter(*args, **kwargs)")
    
    @classmethod
    @abstractmethod
    def _getOutputDFclass(cls, *args, **kwargs) -> ioDF:
        """
        mandatory override: specify the type of ioDF object that will come out of this AxlIO module. Example:

        from axolotl.data import NuclSeqDF

        @classmethod
        @abstractmethod
        def _getOutputDFclass(cls) -> NuclSeqDF:
            return NuclSeqDF

        Use the same *args and **kwargs throughout all _getRecordDelimiter(), _getOutputDFclass() and _parseRecord()
        """
        
        raise NotImplementedError("calling an unimplemented abstract method _getOutputDFclass(*args, **kwargs)")

    @classmethod
    @abstractmethod
    def _parseRecord(cls, record_text: str, *args, **kwargs) -> Dict:
        """
        mandatory override: the main logic of your AxlIO parser. Will receive the string representation
        of your record after being split using the record delimiter. Parse that string and return a
        list of dictionaries (one record can resulted in multiple dataframe rows, for example parsing the
        sequence features in a GenBank file), the key representing the column name of the target ioDF
        class. Remember to use appropriate value data types according to the schema (e.g., a LongType()
        column will only accept integer-type inputs).

        Use the same *args and **kwargs throughout all _getRecordDelimiter(), _getOutputDFclass() and _parseRecord()
        """
        
        raise NotImplementedError("calling an unimplemented abstract method _parseRecord()")

    ##### TO BE OPTIONALLY IMPLEMENTED BY SUBCLASSES #####
    
    @classmethod
    def _prepInput(cls, file_path: str, tmp_dir: str, *args, **kwargs) -> str:
        """
        override this method if you want to provide a pre-processing step into your loadBigFiles()
        method. This function gives you the original file path as an input, and a temporary directory
        path that you can optionally store your pre-processed file into. You will then output the
        file path of that pre-processed file, which will then be read directly by Spark.

        Example: unzipping a fastq.gz file before parsing (assuming gunzip is installed in driver node)

        import subprocess

        classmethod
        def _prepInput(cls, file_path: str, tmp_dir: str) -> str:
            if file_path.endswith(".gz"):
                temp_file = path.join(tmp_dir, "uncompressed.fastq")
                subprocess.run("gunzip " + file_path + " " + temp_file, shell=True)
                return temp_file

        Use the same *args and **kwargs as _getRecordDelimiter(), _getOutputDFclass() and _parseRecord()
        """
        
        # by default, not doing any processing, return the original filepath
        return file_path

    @classmethod
    def _postprocess(cls, data: ioDF, *args, **kwargs) -> ioDF:
        """
        override this method if you want to append additional dataframe-wide operations on the resulting ioDF
        object. Returns the resulting modified ioDF object.

        Use the same *args and **kwargs as _getRecordDelimiter(), _getOutputDFclass() and _parseRecord()
        """
        
        return data