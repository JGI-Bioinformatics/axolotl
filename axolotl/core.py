"""axolotl.core

Contain core classes and functions
"""

from pyspark.sql import DataFrame, Row, types
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id

from axolotl.utils import check_file_exists, is_directory

from abc import ABC, abstractmethod
from os import path
from typing import List, Dict, Tuple
import json
import glob


class AxolotlDF(ABC):
    """Axoltl basic DataFrame class"""

    def __init__(self, df:DataFrame):
        if not self.getSchema() == None and self.__class__.getSchema().jsonValue() != df.schema.jsonValue():
            raise AttributeError((
                "schema conflict on the loaded DataFrame object,"
                " please use schema={}.getSchema() when creating the"
                " pySpark DataFrame object"
            ).format(
                self.__class__.__name__
            ))
        self.df = df
    
    def getMetadata(self) -> dict:
        metadata = {
            "class_name": self.__class__.__name__,
            "schema": self.df.schema.jsonValue()
        }
        return metadata

    @classmethod
    def load(cls, src_parquet:str, num_partitions:int=-1):
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        

        metadata_path = path.join(src_parquet, ".axolotl_metadata.json")
        if not path.exists(metadata_path):
            raise FileNotFoundError("can't find axolotl_metadata.json!")
        else:
            with open(metadata_path) as infile:
                metadata = json.load(infile)
            if metadata["class_name"] != cls.__name__:
                raise TypeError("trying to load {} parquet file into a {}".format(
                    metadata["class_name"],
                    cls.__name__
                ))
            if not cls.getSchema() == None and cls.getSchema().jsonValue() != metadata["schema"]:
                raise AttributeError("schema conflict on the loaded parquet file")
        
        if num_partitions > 0:
            return cls(spark.read.schema(cls.getSchema()).parquet(src_parquet).repartition(num_partitions))
        else:
            return cls(spark.read.schema(cls.getSchema()).parquet(src_parquet))
    
    def store(self, parquet_file_path:str, num_partitions:int=-1):
        if path.exists(parquet_file_path):
            raise Exception("path exists! {}".format(parquet_file_path))
        if num_partitions > 0:
            self.df.repartition(num_partitions).write.option("schema", self.__class__.getSchema()).parquet(parquet_file_path)
        else:
            self.df.write.option("schema", self.__class__.getSchema()).parquet(parquet_file_path)
        metadata_path = path.join(parquet_file_path, ".axolotl_metadata.json")        
        with open(metadata_path, "w") as outfile:
            outfile.write(json.dumps(self.getMetadata()))
    
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


class ioDF(AxolotlDF):
    
    @classmethod
    def getSchema(cls) -> types.StructType:
        return cls._getSchemaSpecific()\
            .add(types.StructField("record_id", types.LongType()))\
            .add(types.StructField("file_path", types.StringType()))
    
    @classmethod
    @abstractmethod
    def _getSchemaSpecific(cls):
        raise NotImplementedError("calling an unimplemented abstract method _getSchemaSpecific()")


class AxolotlIO(ABC):
    """Axolotl basic Input/Output (mostly input) class"""
    
    @classmethod
    def loadSmallFiles(cls, file_pattern:str) -> ioDF:
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")
        sc = spark.sparkContext
        
        # input check
        if not isinstance(file_pattern, str):
            raise TypeError("expected file_pattern to be a string")
            
        return cls._getOutputDFclass()(
            sc.wholeTextFiles(file_pattern)\
            .reduceByKey(lambda row1, row2: row1)\
            .flatMap(cls._parseFile)\
            .toDF(schema=cls._getOutputDFclass().getSchema())
        )        

    @classmethod
    def loadConcatenatedFiles(cls, file_pattern:str, persist:bool=True, intermediate_pq_path:str="") -> ioDF:
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
        df = sc.textFile(file_pattern)\
        .map(lambda x: x[:-1])\
        .filter(lambda x: x != "")\
        .map(lambda x: tuple(x.split(cls._getFileDelimiter()[1], 1)))\
        .reduceByKey(lambda row1, row2: row1)\
        .flatMap(cls._parseFile)\
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
            
        return cls._getOutputDFclass()(df)
        
    @classmethod
    def loadBigFiles(cls, file_paths:List[str], intermediate_pq_path:str) -> ioDF:
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")
        sc = spark.sparkContext
        
        # input check
        if not isinstance(intermediate_pq_path, str):
            raise TypeError("expected intermediate_pq_path to be a string")
        
        # remove double filepaths
        file_paths = set(file_paths)

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
            
            # change delimiter for the custom textFiles() function
            delim_default = sc._jsc.hadoopConfiguration().get("textinputformat.record.delimiter")
            sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", cls._getRecordDelimiter())
            
            # parse each file_path separately and store in the intermediate parquet storage
            for file_path in file_paths:
                print("INFO: parsing big file {}...".format(file_path))
                # check partitioning
                if sc.textFile(file_path).getNumPartitions() == 1:
                    print((
                        "WARNING: loading {} only returned one partition,"
                        " use unzipped files whenever possible to allow splitting files"
                    ).format(file_path))
                # parse
                spark.createDataFrame(
                    sc.textFile(file_path).filter(lambda x: x != "").map(cls._parseRecord),
                    schema=cls._getOutputDFclass()._getSchemaSpecific()
                )\
                .withColumn("record_id", monotonically_increasing_id())\
                .withColumn("file_path", lit(file_path))\
                .write.mode('append').parquet(intermediate_pq_path)            
                
            # revert delimiter back to what it was before
            if delim_default != None:
                sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", delim_default)
            else:
                sc._jsc.hadoopConfiguration().unset("textinputformat.record.delimiter")
            
        # load DF from the intermediate parquet path, then output AxolotlDF
        return cls._getOutputDFclass()(spark.read.parquet(intermediate_pq_path))        

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
        rdd_imported = sc.wholeTextFiles(file_pattern)\
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
    @abstractmethod
    def _parseRecord(cls, text:str) -> List[Dict]:
        raise NotImplementedError("calling an unimplemented abstract method _parseRecord()")

    @classmethod
    def _parseFile(cls, tup:Tuple[str, str]) -> List[Row]:
        file_path, text = tup
        result = []
        for i, record_text in enumerate(
            text.split(cls._getRecordDelimiter())
        ):
            if record_text == "":
                continue
            parsed = cls._parseRecord(record_text)
            if parsed == None:
                continue
            result.append({
                **cls._parseRecord(record_text),
                **{
                    "record_id": i,
                    "file_path": file_path
                }
            })
        return result
    