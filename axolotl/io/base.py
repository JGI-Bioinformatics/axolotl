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
    
    """
    
    @classmethod
    def loadSmallFiles(cls, file_pattern: str, params: Dict={}) -> ioDF:
        """

        """

        spark, sc = get_spark_session_and_context()
        
        # input check
        if not isinstance(file_pattern, str):
            raise TypeError("expected file_pattern to be a string")
            
        return cls.__postprocess(cls._getOutputDFclass()(
            sc.wholeTextFiles(file_pattern)\
                .flatMap(lambda x: cls._parseFile(x[0], x[1], params))\
                .toDF(schema = cls._getOutputDFclass()._getSchema())
        ), params = params)

    @classmethod
    def loadConcatenatedFiles(cls, file_pattern: str, persist: bool=True, intermediate_pq_path: str="", params:Dict={}) -> ioDF:
        """
        
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
            .flatMap(lambda x: cls._parseFile(x[0], x[1], params))\
            .toDF(schema = cls._getOutputDFclass()._getSchema())
        
        if persist:
            df.persist()
            if intermediate_pq_path == "":
                df.count()
            else:
                df.write.parquet(intermediate_pq_path)
        elif intermediate_pq_path != "":
            df.write.parquet(intermediate_pq_path)
            df = spark.read.parquet(intermediate_pq_path)

        # revert delimiter back to what it was before
        if delim_default != None:
            sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", delim_default)
        else:
            sc._jsc.hadoopConfiguration().unset("textinputformat.record.delimiter")
            
        return cls.__postprocess(cls._getOutputDFclass()(df), params=params)
        
    @classmethod
    def loadBigFiles(cls, file_paths:List[str], intermediate_pq_path:str, params:Dict={}) -> ioDF:
        """
        
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
                    text_file_path = cls._prepInput(file_path, tmp_dir)
                    if use_dbfs:
                        text_file_path = text_file_path.replace("/dbfs/", "dbfs:/", 1)
                    
                    # change delimiter for the custom textFiles() function
                    delim_default = sc._jsc.hadoopConfiguration().get("textinputformat.record.delimiter")
                    sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", cls._getRecordDelimiter())
                    # parse
                    _df = spark.createDataFrame(
                        sc.textFile(text_file_path).filter(lambda x: x != "").map(
                            lambda y: cls._parseRecord(y, params)
                        ).flatMap(lambda n: n).filter(lambda z: (z != None) if params.get("skip_malformed_record", False) else True),
                        schema=cls._getOutputDFclass()._getSchemaSpecific()
                    )
                    _orig_cols = _df.columns
                    _df.withColumn("file_path", when(lit(True), lit(parse_path_type(file_path)["path"])))\
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
        return cls.__postprocess(cls._getOutputDFclass()(spark.read.parquet(intermediate_pq_path)), params=params)

    @classmethod
    def concatSmallFiles(cls, file_pattern: str, path_output: str, num_partitions: int=-1):
        """
        
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
    def _getFileDelimiter(cls) -> Tuple[str, str]:
        """
        
        """

        return ("\n>>>>>file_path:", "\n")
    
    @classmethod
    def _parseFile(cls, file_path: str, text: str, params: Dict={}) -> List[Row]:
        """
        
        """

        result = []
        for record_text in text.split(cls._getRecordDelimiter()):
            if record_text == "":
                continue
            parsed_array = cls._parseRecord(record_text, params)
            for parsed in parsed_array:
                if parsed == None and params.get("skip_malformed_record", False):
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
    def __postprocess(cls, data:ioDF, params:Dict={}):
        """
        
        """

        data = cls._postprocess(data, params)
        return data

    ##### TO BE IMPLEMENTED BY SUBCLASSES #####

    @classmethod
    @abstractmethod
    def _getRecordDelimiter(cls) -> str:
        """
        
        """
        
        raise NotImplementedError("calling an unimplemented abstract method _getRecordDelimiter()")
    
    @classmethod
    @abstractmethod
    def _getOutputDFclass(cls) -> ioDF:
        """
        
        """
        
        raise NotImplementedError("calling an unimplemented abstract method _getOutputDFclass()")

    @classmethod
    @abstractmethod
    def _parseRecord(cls, text: str, params:Dict={}) -> Dict:
        """
        
        """
        
        raise NotImplementedError("calling an unimplemented abstract method _parseRecord()")

    ##### TO BE OPTIONALLY IMPLEMENTED BY SUBCLASSES #####
    
    @classmethod
    def _prepInput(cls, file_path: str, tmp_dir: str) -> str:
        """
        
        """
        
        # by default, not doing any processing, return the original filepath
        return file_path

    @classmethod
    def _postprocess(cls, data: ioDF, params: Dict={}) -> ioDF:
        """
        
        """
        
        return data