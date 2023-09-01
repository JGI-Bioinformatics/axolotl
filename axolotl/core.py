"""axolotl.core

Contain core classes and functions
"""

from pyspark.sql import DataFrame, Row, types
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id, when

from axolotl.utils.file import check_file_exists, is_directory, make_dirs, fopen, parse_path_type, get_temp_dir

from abc import ABC, abstractmethod
from os import path
from typing import List, Dict, Tuple
import json
import glob


class AxlDF(ABC):
    """Axoltl basic DataFrame class
    
    Overload the pySpark's dataframe so that it contains metadata to describe the dataframe. Each column can still have its own metadata.
    Load/store AxlDF handle both data and metadata.

    Example:
    
    """

    def __init__(self, df:DataFrame):
        if (self.getSchema() is not None) and (self.__class__.getSchema().jsonValue() != df.schema.jsonValue()):
            raise AttributeError(f"schema conflict on the loaded DataFrame object, please use schema={self.__class__.__name__}.getSchema() when creating the pySpark DataFrame object")
        self.df = df
    
    def getMetadata(self) -> dict:
        metadata = {
            "class_name": self.__class__.__name__,
            "schema": self.df.schema.jsonValue()
        }
        return metadata

    @classmethod
    def load(cls, src_parquet:str, num_partitions:int=200):
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        

        metadata_path = path.join(src_parquet, ".axolotl_metadata.json")
        if not check_file_exists(metadata_path):
            raise FileNotFoundError("can't find axolotl_metadata.json!")
        else:
            with fopen(metadata_path) as infile:
                metadata = json.load(infile)
            if metadata["class_name"] != cls.__name__:
                raise TypeError(f"trying to load {metadata['class_name']} parquet file into a {cls.__name__}")
            if (cls.getSchema() is not None) and (cls.getSchema().jsonValue() != metadata["schema"]):
                raise AttributeError("schema conflict on the loaded parquet file")
        
        used_schema = cls.getSchema()
        if used_schema == None:
            used_schema = types.StructType.fromJson(metadata["schema"])

        if num_partitions !=200:
            return cls(spark.read.schema(used_schema).parquet(src_parquet).repartition(num_partitions))
        else:
            return cls(spark.read.schema(used_schema).parquet(src_parquet))
    
    def store(self, parquet_file_path:str, overwrite=False, num_partitions:int=200) -> None:
        if check_file_exists(parquet_file_path) and (not overwrite):
            raise Exception(f"path exists! {parquet_file_path}, please set overwrite to True.")
        if num_partitions != 200:
            self.df.repartition(num_partitions).write.mode("overwrite").option("schema", self.__class__.getSchema()).parquet(parquet_file_path)
        else:
            self.df.write.mode("overwrite").option("schema", self.__class__.getSchema()).parquet(parquet_file_path)
        metadata_path = path.join(parquet_file_path, ".axolotl_metadata.json")        
        with fopen(metadata_path, "w") as outfile:
            json.dump(self.getMetadata(), outfile)

    
    @classmethod
    @abstractmethod
    def getSchema(cls) -> types.StructType:
        """return: DF schema"""
        raise NotImplementedError("calling an unimplemented abstract method getSchema()")
    
    @classmethod
    @abstractmethod
    def validateRow(cls, row: Row) -> bool:
        """return: validated/not"""
        raise NotImplementedError("calling an unimplemented abstract method validateRow()")
    
    def filterValids(self) -> DataFrame:
        return self.__class__(
            self.df.rdd\
            .filter(self.__class__.validateRow)\
            .toDF(schema=self.__class__.getSchema())
        )

    @classmethod
    def validateRowNot(cls, row: Row) -> bool:
        return not cls.validateRow(row)
    
    def filterNotValids(self) -> DataFrame:
        return self.__class__(
            self.df.rdd\
            .filter(self.__class__.validateRowNot)\
            .toDF(schema=self.__class__.getSchema())
        )
                
    def countValids(self) -> Tuple[int, int]:
        return self.df.rdd.map(self.__class__.validateRow).aggregate(
            (0, 0),
            lambda x, y: (x[0] + 1, x[1]) if y else (x[0], x[1] + 1),
            lambda x, y: (x[0] + y[0], x[1] + y[1])
        )


class ioDF(AxlDF):
    
    @classmethod
    def getSchema(cls) -> types.StructType:
        return_type = cls._getSchemaCommon()
        for field in cls._getSchemaSpecific():
            return_type = return_type.add(field)
        return return_type
    
    @classmethod
    def _getSchemaCommon(cls):
        return types.StructType([
            types.StructField("file_path", types.StringType()),
            types.StructField("row_id", types.LongType())
        ])

    @classmethod
    @abstractmethod
    def _getSchemaSpecific(cls):
        raise NotImplementedError("calling an unimplemented abstract method _getSchemaSpecific()")


class MetaDF(ioDF):
    """dataframe to handle 'metadata', i.e., key-value pairs of a file"""
        
    @classmethod
    def _getSchemaSpecific(cls) -> types.StructType:
        return types.StructType([
            types.StructField("key", types.StringType()),
            types.StructField("value", types.StringType())
        ])
        
    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True


class AxlIO(ABC):
    """Axolotl basic Input/Output (mostly input) class"""
    
    @classmethod
    def loadSmallFiles(cls, file_pattern:str, minPartitions:int=200, params:Dict={}) -> ioDF:
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")
        sc = spark.sparkContext
        
        # input check
        if not isinstance(file_pattern, str):
            raise TypeError("expected file_pattern to be a string")
            
        return cls.__postprocess(cls._getOutputDFclass()(
            sc.wholeTextFiles(file_pattern, minPartitions=minPartitions)\
            .flatMap(lambda x: cls._parseFile(x[0], x[1], params))\
            .toDF(schema=cls._getOutputDFclass().getSchema())
        ), params=params)

    @classmethod
    def loadConcatenatedFiles(cls, file_pattern:str, minPartitions:int=200, persist:bool=True, intermediate_pq_path:str="", params:Dict={}) -> ioDF:
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")
        sc = spark.sparkContext
        
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
        df = sc.textFile(file_pattern, minPartitions=minPartitions)\
        .map(lambda x: x[:-1])\
        .filter(lambda x: x != "")\
        .map(lambda x: tuple(x.split(cls._getFileDelimiter()[1], 1)))\
        .flatMap(lambda x: cls._parseFile(x[0], x[1], params))\
        .toDF(schema=cls._getOutputDFclass().getSchema())
        
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
    def loadBigFiles(cls, file_paths:List[str], intermediate_pq_path:str, minPartitions:int=200, params:Dict={}) -> ioDF:
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")
        sc = spark.sparkContext
        
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
                        sc.textFile(text_file_path, minPartitions=minPartitions).filter(lambda x: x != "").map(
                            lambda y: cls._parseRecord(y, params)
                        ).flatMap(lambda n: n).filter(lambda z: (z != None) if params.get("skip_malformed_record", False) else True),
                        schema=cls._getOutputDFclass()._getSchemaSpecific()
                    )
                    _orig_cols = _df.columns
                    _df.withColumn("file_path", lit(parse_path_type(file_path)["path"]))\
                        .withColumn("row_id", lit(0))\
                        .select(["file_path", "row_id"] + _orig_cols)\
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
    def concatSmallFiles(cls, file_pattern:str, path_output:str, num_partitions:int=-1):
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")
        sc = spark.sparkContext
        
        # make sure the outputh path is empty
        if check_file_exists(path_output):
            raise Exception("path_output file path exists! {}".format(path_output))
        if not isinstance(file_pattern, str):
            raise TypeError("expected file_pattern to be a string")
        
        # import RDD
        rdd_imported = sc.wholeTextFiles(file_pattern, minPartitions=num_partitions)\
        .reduceByKey(lambda row1, row2: row1)\
        .map(lambda x: cls._getFileDelimiter()[0] + x[0] + cls._getFileDelimiter()[1] + x[1])\
        
        if num_partitions > 0:
            rdd_imported = rdd_imported.repartition(num_partitions)            
        
        rdd_imported.saveAsTextFile(path_output)
    
    @classmethod
    def _getFileDelimiter(cls) -> Tuple[str, str]:
        return ("\n>>>>>file_path:", "\n")
    
    @classmethod
    @abstractmethod
    def _getRecordDelimiter(cls) -> str:
        raise NotImplementedError("calling an unimplemented abstract method _getRecordDelimiter()")
    
    @classmethod
    @abstractmethod
    def _getOutputDFclass(cls) -> ioDF:
        raise NotImplementedError("calling an unimplemented abstract method _getOutputDFclass()")
    
    @classmethod
    def _prepInput(cls, file_path:str, tmp_dir:str) -> str:
        # by default, not doing any processing, return the original filepath
        return file_path

    @classmethod
    @abstractmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        raise NotImplementedError("calling an unimplemented abstract method _parseRecord()")

    @classmethod
    def _parseFile(cls, file_path:str, text:str, params:Dict={}) -> List[Row]:
        result = []
        for i, record_text in enumerate(
            text.split(cls._getRecordDelimiter())
        ):
            if record_text == "":
                continue
            parsed_array = cls._parseRecord(record_text, params)
            for parsed in parsed_array:
                if parsed == None and params.get("skip_malformed_record", False):
                    continue
                if isinstance(parsed, list):
                    result.append([parse_path_type(file_path)["path"], i] + parsed)
                elif isinstance(parsed, dict):
                    result.append({ # the ** is to concatenate the two dictionaries
                        **{
                            "file_path": file_path,
                            "row_id": i
                        },
                        **parsed
                    })
                else:
                    raise NotImplementedError("_parseFile needs to return either lists of list or lists of dict")
                
                i += 1 # TODO: this implementation will NOT produce uniqe IDs across partitions!!
        return result
    
    @classmethod
    def __postprocess(cls, data:ioDF, params:Dict={}):
        data.df = data.df.withColumn("row_id", when(lit(True), monotonically_increasing_id()))
        data = cls._postprocess(data, params)
        return data
    
    @classmethod
    def _postprocess(cls, data:ioDF, params:Dict={}) -> Dict:
        return data


class AxlSet(ABC):
    """base class for holding record-type objects, which serve as a 'dataset' of multiple linked AxlDFs"""

    _data = {} # this holds all the AxlDF objects i.e., dataset tables; don't modify this directly
    def get(self, key:str) -> AxlDF:
        return self._data[key]

    def getMetadata(self) -> Dict:
        metadata = {
            "class_name": self.__class__.__name__
        }
        return metadata

    def __init__(self, imported_data):
        for key, data_class in self.__class__._dataDesc().items():
            if key not in imported_data:
                raise Exception("need data -> '{}'".format(key))
            elif data_class != imported_data[key].__class__:
                raise Exception("data '{}' have format conflict".format(key))
            self._data[key] = imported_data[key]

    @classmethod
    @abstractmethod
    def _dataDesc(cls) -> Dict:
        raise NotImplementedError("calling an unimplemented abstract method _dataDesc()")
        
    @classmethod
    def load(cls, file_path:str, num_partitions:int=200):
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        

        metadata_path = path.join(file_path, "_metadata.json")
        if not check_file_exists(metadata_path):
            raise FileNotFoundError("can't find _metadata.json!")
        else:
            with fopen(metadata_path) as infile:
                metadata = json.load(infile)
            if metadata["class_name"] != cls.__name__:
                raise TypeError("loaded class {} doesn't match the target class {}".format(
                    metadata["class_name"],
                    cls.__name__
                ))
                
            data_folder = path.join(file_path, "data")
            imported_data = {}
            for key, data_class in cls._dataDesc().items():
                df_path = path.join(data_folder, key)
                if not check_file_exists(df_path):
                    raise FileNotFoundError("can't data folder {}!".format(key))
                imported_data[key] = data_class.load(df_path, num_partitions)
                
            return cls(imported_data)
    
    def store(self, file_path:str, num_partitions:int=200, overwrite=False):
        if check_file_exists(file_path) and (not overwrite):
            raise Exception(f"path exists! {file_path} Please set overwrite to True.")
        
        data_folder = path.join(file_path, "data")
        if not overwrite:
            make_dirs(file_path)
            make_dirs(data_folder)
        
        for key, table in self._data.items():
            table.store(path.join(data_folder, key), num_partitions=num_partitions, overwrite=overwrite)
            
        metadata_path = path.join(file_path, "_metadata.json")        
        with fopen(metadata_path, "w") as outfile:
            outfile.write(json.dumps(self.getMetadata()))


class setIO(AxlIO):
    """base class for implementing AxlSet-based files parsing, this wraps multiple different AxlIO into one"""
    
    @classmethod
    def loadSmallFiles(cls, file_pattern:str, minPartitions:int=200, params:Dict={}) -> ioDF:
        imported_data = {}
        for key, data_class in cls._getOutputIOclasses().items():
            imported_data[key] = data_class.loadSmallFiles(file_pattern, minPartitions=minPartitions, params=params)
        return cls._getOutputDFclass()(imported_data)

    @classmethod
    def loadConcatenatedFiles(cls, file_pattern:str, minPartitions:int=200, persist:bool=True, intermediate_pq_path:str="", params:Dict={}) -> ioDF:
        imported_data = {}
        for key, data_class in cls._getOutputIOclasses().items():
            imported_data[key] = data_class.loadConcatenatedFiles(file_pattern, persist, intermediate_pq_path + "." + key, minPartitions=minPartitions, params=params)
        return cls._getOutputDFclass()(imported_data)
        
    @classmethod
    def loadBigFiles(cls, file_paths:List[str], intermediate_pq_path:str, minPartitions:int=200, params:Dict={}) -> ioDF:
        imported_data = {}
        for key, data_class in cls._getOutputIOclasses().items():
            imported_data[key] = data_class.loadBigFiles(file_paths, intermediate_pq_path + "." + key, params=params, minPartitions=minPartitions)
        return cls._getOutputDFclass()(imported_data)

    @classmethod
    @abstractmethod
    def _getOutputIOclasses(cls) -> Dict:
        raise NotImplementedError("calling an unimplemented abstract method _getOutputIOclass()")
        
    @classmethod
    @abstractmethod
    def _getOutputDFclass(cls) -> AxlSet:
        raise NotImplementedError("calling an unimplemented abstract method _getOutputDFclass()")
    
    @classmethod
    def _parseRecord(cls, text:str, params:Dict={}) -> Dict:
        raise NotImplementedError("can't call _parseRecord() directly from a setIO object")

    @classmethod
    def _parseFile(cls, file_path:str, text:str, params:Dict={}) -> List[Row]:
        raise NotImplementedError("can't call _parseFile() directly from a setIO object")


class FlexDF(ioDF):
    """base class for 'flexible' dataframes, i.e., where users can specify their own column schema"""

    def getMetadata(self) -> dict:
        metadata = super().getMetadata()
        metadata["class_name"] = FlexDF.__name__
        return metadata

    @classmethod
    def validateRow(cls, row: Row) -> bool:
        return True

    @classmethod
    def _getSchemaSpecific(cls) -> types.StructType:
        raise NotImplementedError("calling an unimplemented abstract method _getSchemaSpecific()")
    
    @classmethod
    def getSchema(cls) -> types.StructType:
        return None


class FlexIO(AxlIO):
    """base class for handling flexible tabular inputs, where column schema is user defined"""
    
    @classmethod
    def _getFlexDFclass(cls, colSchema:types.StructType) -> FlexDF:
        class _tempFlexDF(FlexDF):
            @classmethod
            def getSchema(cls) -> types.StructType:
                return_type = ioDF._getSchemaCommon()
                for field in cls._getSchemaSpecific():
                    return_type = return_type.add(field)
                return return_type
            @classmethod
            def _getSchemaSpecific(cls) -> types.StructType:
                return colSchema
        return _tempFlexDF

    @classmethod
    def _getFlexIOclass(cls, colSchema:types.StructType) -> AxlIO:
        class _tempFlexIO(AxlIO):
            @classmethod
            def _getRecordDelimiter(clsI) -> str:
                return cls._getRecordDelimiter()

            @classmethod
            def _getOutputDFclass(clsI) -> ioDF:
                return FlexIO._getFlexDFclass(colSchema)

            @classmethod
            def _parseRecord(clsI, text:str, params:Dict={}) -> Dict:
                return cls._parseRecord(text, params)
        return _tempFlexIO

    @classmethod
    def loadSmallFiles(cls, file_pattern:str, colSchema:types.StructType, minPartitions:int=200, params:Dict={}) -> ioDF:
        return cls._getFlexIOclass(colSchema).loadSmallFiles(file_pattern, minPartitions=minPartitions, params=params)

    @classmethod
    def loadConcatenatedFiles(cls, file_pattern:str, colSchema:types.StructType, minPartitions:int=200, persist:bool=True, intermediate_pq_path:str="", params:Dict={}) -> ioDF:
        return cls._getFlexIOclass(colSchema).loadConcatenatedFiles(file_pattern, minPartitions=minPartitions, persist=persist, intermediate_pq_path=intermediate_pq_path, params=params)
            
    @classmethod
    def loadBigFiles(cls, file_paths:List[str], intermediate_pq_path:str, colSchema:types.StructType, minPartitions:int=200, params:Dict={}) -> FlexDF:                
        return cls._getFlexIOclass(colSchema).loadBigFiles(file_paths, intermediate_pq_path, minPartitions, params)
