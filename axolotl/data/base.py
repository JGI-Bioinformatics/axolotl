from abc import ABC, abstractmethod
from os import path
from typing import Tuple, Dict, List
import json

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from axolotl.utils.file import check_file_exists, fopen
from axolotl.utils.spark import get_spark_session_and_context


class AxlDF(ABC):
    """
    Axolotl's basic DataFrame class
    
    This wraps the vanilla pyspark's DataFrame and incorporate statically-typed, subclass-specific
    schema definition. Additionally, it will also include a mandatory column: "idx" that will be calculated
    using "monotonically_increasing_id()" at object's instantiation.

    To subclass, inherit everything under "##### TO BE IMPLEMENTED BY SUBCLASSES #####"

    Example subclassing:

    class NuclSeqDF(AxlDF):

        @classmethod
        def getSchema(cls) -> T.StructType:
            return T.StructType([
                T.StructField('seq_id', T.StringType()),
                T.StructField('desc', T.StringType()),
                T.StructField('sequence', T.StringType())
            ])
        
        @classmethod
        @abstractmethod
        def validateRow(cls, row: Row) -> bool:
            allowed_letters = "ATGCNatgcn"
            return (F.all(c in allowed_letters for c in row["sequence"]))
        
    Example instantiation:
    
    pyspark_df = sc.parallelize([{"seq_id": "ctg_001", "desc": "contig 1", "seq": "ATGCATGC"}]).toDF()
    axolotl_df = NuclSeqDF(pyspark_df)
    axolotl_df.countValids()

    """

    def __init__(self, df: DataFrame, override_idx: bool=False, keep_idx: bool=False, loaded_metadata: Dict=None, sources: List=[]):
        """
        by default, an AxlDF instance can be started by supplying a corresponding pySpark DataFrame with correct schema.
        If a column 'idx' already exists in the supplied dataframe, users can choose between regenerating a new idx (guaranteed
        to be unique) or keep the old idx.
        """
        orig_columns = df.columns
        generate_new_idx = True
        if "idx" in orig_columns:
            if not override_idx and not keep_idx:
                raise ValueError(
                    "the passed pyspark DataFrame already has an 'idx' column (please use 'override_idx=True'"
                    " or 'keep_idx=True' to select the appropriate action)"
                )
            elif keep_idx: # if both keep and override are selected, prioritize keep
                generate_new_idx = False
            elif override_idx:
                orig_columns.remove("idx")

        if generate_new_idx:
            df = df.withColumn("idx", F.when(F.lit(True), F.monotonically_increasing_id()))

        df = df.select(self.__class__.getSchema().fieldNames())
        if (self.__class__.getSchema() is not None) and (self.__class__.getSchema().jsonValue() != df.schema.jsonValue()):
            raise AttributeError(
                "schema conflict on the loaded DataFrame object, please use "
                f"schema={self.__class__.__name__}.getSchema() when creating the pySpark DataFrame object"
            )

        self._df = df
        if loaded_metadata:
            self._id = loaded_metadata["id"]
            self._sources = loaded_metadata["source_ids"]
        else:
            self._id = "{}#{}".format(self.__class__.__name__, id(self.df))
            self._sources = [source._id for source in sources]

    @property
    def df(self):
        """
        getter to fetch the underlying pySpark DataFrame
        """
        return self._df

    @df.setter
    def df(self, new_df):
        """
        update the underlying pySpark DataFrame, also, update
        the AxlDF's id since now the data has been changed.
        """
        self._df = new_df
        self._id = "{}#{}".format(self.__class__.__name__, id(self.df))

    @df.deleter
    def df(self):
        """
        deleter function for pySpark DataFrame
        """
        del self._df

    def updateSources(self, sources: List):
        """
        update the _sources attribute post object instantiation
        """
        self._sources = [source._id for source in sources]

    @classmethod
    def getSchema(cls) -> T.StructType():
        """
        call this function to get the complete pyspark schema of an AxlDF class
        """
        _schema = T.StructType([
            T.StructField("idx", T.LongType())
        ])
        for field in cls._getSchema():
            _schema = _schema.add(field)
        return _schema

    @classmethod
    def read(cls, src_parquet: str, num_partitions: int=None):
        """
        reads a previously-written AxlDF instance
        """
        spark, sc = get_spark_session_and_context()      

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

        df = spark.read.schema(used_schema).parquet(src_parquet)
        if num_partitions:
            df =  df.repartition(num_partitions)

        return cls(df, keep_idx=True, loaded_metadata=metadata)
    
    def write(self, parquet_file_path: str, overwrite: bool=False, num_partitions: int=None) -> None:
        """
        write an AxlDF instance into a Parquet-like folder structure. It will contains both the Parquet
        'fragment' files and a corresponding .axolotl_metadata.json file to support loading back into the
        correct AxlDF object.
        """
        if check_file_exists(parquet_file_path):
            if overwrite:
                # check if need to update the DF
                metadata_path = path.join(parquet_file_path, ".axolotl_metadata.json")
                if not check_file_exists(metadata_path):
                    raise FileNotFoundError("can't find axolotl_metadata.json!")
                else:
                    with fopen(metadata_path) as infile:
                        metadata = json.load(infile)
                if metadata["id"] != self._id:
                    df = self.df
                    if num_partitions:
                        df = df.repartition(num_partitions)
                    df.persist() # need this because we're overriding the source parquet file
                    df.count()
                    df.write.mode("overwrite").option("schema", self.__class__.getSchema()).parquet(parquet_file_path)
            else:
                raise Exception(f"path exists! {parquet_file_path}, please set overwrite to True.")
        else:
            df = self.df
            if num_partitions:
                df = df.repartition(num_partitions)
            df.write.option("schema", self.__class__.getSchema()).parquet(parquet_file_path)

        metadata_path = path.join(parquet_file_path, ".axolotl_metadata.json")        
        with fopen(metadata_path, "w") as outfile:
            metadata = {
                "id": self._id,
                "source_ids": self._sources,
                "class_name": self.__class__.__name__,
                "schema": self.df.schema.jsonValue()
            }
            json.dump(metadata, outfile)

    @classmethod
    def __validateRowNot(cls, row: Row) -> bool:
        return not cls.validateRow(row)

    def filterValids(self) -> DataFrame:
        """
        return a pyspark DataFrame with only validated rows
        """
        return self.__class__(
            self.df.rdd\
            .filter(self.__class__.validateRow)\
            .toDF(schema=self.__class__.getSchema())
        )
    
    def filterNotValids(self) -> DataFrame:
        """
        return a pyspark DataFrame with only non-validated rows
        """
        return self.__class__(
            self.df.rdd\
            .filter(self.__class__.validateRowNot)\
            .toDF(schema=self.__class__.getSchema())
        )
                
    def countValids(self) -> Tuple[int, int]:
        """
        count the total number of valid rows, return a tuple of
        (valid_row_count, invalid_row_count)
        """
        return self.df.rdd.map(self.__class__.validateRow).aggregate(
            (0, 0),
            lambda x, y: (x[0] + 1, x[1]) if y else (x[0], x[1] + 1),
            lambda x, y: (x[0] + y[0], x[1] + y[1])
        )

    ##### TO BE IMPLEMENTED BY SUBCLASSES #####

    @classmethod
    @abstractmethod
    def _getSchema(cls) -> T.StructType:
        """
        define your AxlDF's subclass schema by returning a pyspark's StructType here.
        This schema will then be enforced throughout all of your classes' objects.

        IMPORTANT NOTE: please refrain from using 'idx' (or any other columns already defined
        by the top-level AxlDF classes you inherit) as column names to avoid schema conflicts.
        """
        raise NotImplementedError("calling an unimplemented abstract method _getSchema()")
    
    @classmethod
    @abstractmethod
    def validateRow(cls, row: Row) -> bool:
        """
        return a True/False depending or wheter a Row from your dataframe is valid / not.
        For example, you can check whether the sequence in your fastq dataframe contain only
        nucleotide letters.
        """
        raise NotImplementedError("calling an unimplemented abstract method validateRow()")


###### first-level specific AxlDFs ######
    

class ioDF(AxlDF):
    """
    Axolotl's IO-related DataFrame base class

    Adding an additional mandatory column "file_path" that will point to the source
    raw file path (typically coming from AxlIO modules) related to the dataframe rows.

    Example subclassing:

    class NuclSeqDF(ioDF):

        @classmethod
        def getSchema(cls) -> T.StructType:
            return T.StructType([
                T.StructField('seq_id', T.StringType()),
                T.StructField('desc', T.StringType()),
                T.StructField('sequence', T.StringType())
            ])
        
        @classmethod
        @abstractmethod
        def validateRow(cls, row: Row) -> bool:
            allowed_letters = "ATGCNatgcn"
            return (F.all(c in allowed_letters for c in row["sequence"]))
        
    Example instantiation: # notice the added "file_path" value
    
    pyspark_df = sc.parallelize([{"seq_id": "ctg_001", "file_path": "test.fna", "desc": "contig 1", "seq": "ATGCATGC"}]).toDF()
    axolotl_df = NuclSeqDF(pyspark_df)
    axolotl_df.countValids()

    """
    
    @classmethod
    def _getSchema(cls) -> T.StructType:
        """
        override AxlDF's base _getSchema and adds a "file_path" column, then combine it with
        subclasses' _getSchemaSpecific() columns.
        """
        _schema = T.StructType([
            T.StructField("file_path", T.StringType())
        ])
        for field in cls._getSchemaSpecific():
            _schema = _schema.add(field)
        return _schema

    ##### TO BE IMPLEMENTED BY SUBCLASSES #####
    
    @classmethod
    @abstractmethod
    def _getSchemaSpecific(cls):
        """
        define your ioDF's subclass schema by returning a pyspark's StructType here.
        This schema will then be enforced throughout all of your classes' objects.

        IMPORTANT NOTE: please refrain from using 'idx' and 'file_path' (or any other columns
        already defined by the top-level AxlDF classes you inherit) as column names to avoid
        schema conflicts.
        """
        raise NotImplementedError("calling an unimplemented abstract method _getSchemaSpecific()")