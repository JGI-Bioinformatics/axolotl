from abc import ABC, abstractmethod
from os import path
from typing import Tuple, Dict
import json

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from axolotl.data import AxlDF

class AxlApp:
    """
    Axolotl's base class for constructing an omics data analysis "app". An AxlApp contains
    a relational database-like structure (each "table" being a specific AxlDF object) and
    and a collection of callable functions / procedures. Each AxlApp subclass will be created
    via its "getOrCreate()" function, which takes subclass-specific parameters to prepare
    the database structure in the provided app_folder_path.
    
    To subclass, inherit everything under "##### TO BE IMPLEMENTED BY SUBCLASSES #####"

    See existing implementations within the axolotl/app subfolder to get an idea how to implement
    a new AxlApp class.

    Example subclassing:
    
    class ExampleApp(AxlApp):

        @classmethod
        @abstractmethod
        def _dataDesc(cls) -> Dict:
            # describe the app's "database" structure, in this case
            # it will have two tables: "contigs" (a NuclSeqDF) and
            # "cds" (a cdsDF). A relationship between "cds" and "contigs"
            # is established via cds.source_path == contigs.file_path
            # and cds.seq_id == contigs.seq_id (see NuclSeqDF and cdsDF
            # schema for details)

            return {
                "contigs": NuclSeqDF,
                "cds": cdsDF
            }

        @abstractmethod
        def _creationFunc(self, genbanks_path: str):
            # we override the *args and **kwargs with our own class-specific
            # parameters. In this case an exampleApp will be created by
            # supplying the path (in glob pattern) of genbank file of the
            # input genomes. Then, in this _creationFunc method, we will use
            # gbkSeqIO and gbkFeatIO's loadSmallFiles() functions to parse
            # and store the "contigs" and "cds" table of the app.

            contigs_df = gbkSeqIO.loadSmallFiles(genbanks_path)
            self._setData("contigs", contigs_df) # associate AxlDF with the table
            self._saveData("contigs") # execute and store the actual spark query

            feature_df = gbkFeatIO.loadSmallFiles(genbanks_path)
            cds_df = cdsDF.fromRawFeatDF(feature_df, contigs_df)
            self._setData("cds", cds_df)
            self._saveData("cds")

            # to demonstrate the additional flexibility of AxlApp, we create a
            # pickle file that we can use to load an extra data within the ExampleApp
            # object.
            self._dummy_metadata = "just_a_string"
            with open(path.join(self.__folder_path, "dummy.pkl"), "wb") as file:
                pickle.dump(self._dummy_metadata, file)
            
        @abstractmethod
        def _loadExtraData(self):
            # here, we can load back the _dummy_metadata variable from the pickled file
            with open(path.join(self.__folder_path, "dummy.pkl"), "rb") as file:
                self._dummy_metadata = pickle.load(file)

        def getCDScount(self):
            # a demonstration of app-specific functions. With this method, users can
            # query the previosly-created database and get a tally of CDS counts per
            # genomes (i.e., gbk path) as a pySpark DataFrame.

            return self._getData("cds").df.groupBy("source_path").count()

    Example usage:

    > if first-time creation, parse the genome file paths and create "contigs" and "cds"
    > database. If ./my_genomes_app were previously-processed, load the app directly instead
    
    my_app = ExampleApp.getOrCreate("./my_genomes_app", "./my_genome_files/*.gbk")

    > after loading the app, now you can use its functions..

    my_app.getCDScount().show()

    > ..or directly work with its dataframes
    
    my_app.getData("contigs").count()

    """

    def __init__(self, app_folder_path: str, metadata: Dict):
        """
        do not instantiate AxlApp directly, instead use the appropriate getOrCreate() function
        """

        self.__data = {}
        self.__folder_path = app_folder_path
        self.__metadata = metadata

    @classmethod
    def getOrCreate(cls, app_folder_path: str, *args, **kwargs):
        """
        the main entry point for creating a new AxlApp object. given a specific path, Axolotl will
        either load the previously-stored app data (if folder exists) or create and save a new app
        object.

        Don't inherit this method directly, instead apply your subclass-specific logic in the _create()
        method.
        """

        app_object = cls.__loadFolder(app_folder_path)
        if not wf_object:
            print("can't load  from folder, creating a new object")
            app_object = cls.__createAndSave(app_folder_path, *args, **kwargs)        
        if not app_object:
            raise Exception("failed to load or create object")
        return app_object

    def getData(self, key: str):
        """
        call this method to work directly with the AxlDF object stored in the app.
        """

        return self._getData(key)
        
    def _getData(self, key: str):
        """
        getter for app's AxlDF objects, meant to be used within the _create() method
        """

        return self.__data[key]

    def _setData(self, key: str, df: AxlDF):
        """
        setter for app's AxlDF objects, meant to be used within the _create() method
        """

        self.__data[key] = df

    def _saveData(self, key: str, overwrite: bool=False):
        """
        call this after creating the appropriate AxlDF object for the app's data
        """

        self.__data[key].store(path.join(self.__folder_path, "data", key), overwrite=overwrite)

    @classmethod
    def __createAndSave(cls, app_folder_path: str, *args, **kwargs):
        """
        internal class method that handles the creation of AxlApp folder and calling the specific
        sub-class _creationFunc() methods.
        """

        # create a new empty data dir structure, along with metadata
        if check_file_exists(wf_folder_path):
            raise Exception("Trying to create a new AxlApp folder but folder exists! {}".format(app_folder_path))
        created_obj = cls(app_folder_path, {
            "class_name": cls.__name__
        })
        make_dirs(app_folder_path)
        make_dirs(path.join(app_folder_path, "data"))
        make_dirs(path.join(app_folder_path, "extra_data"))
        created_obj.__saveMetadata()
        # implement subclass-specific logic
        created_obj._creationFunc(*args, **kwargs)
        return created_obj

    @classmethod
    def __loadFolder(cls, app_folder_path: str):
        """
        internal class method that handles loading an existing AxlApp folder including
        the app-specific logic (via _loadExtraData()) and returning the AxlApp object.
        """

        loaded_obj = None
        if check_file_exists(app_folder_path): # check if folder exists
            # check metadata and load __data variable
            metadata_path = path.join(app_folder_path, "_metadata.json")
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
                loaded_obj = cls(app_folder_path, metadata)
                data_folder = path.join(app_folder_path, "data")
                for key, data_class in cls._dataDesc().items():
                    df_path = path.join(data_folder, key)
                    if not check_file_exists(df_path):
                        raise FileNotFoundError("can't find data folder {}!".format(key))
                    loaded_obj._setData(key, data_class.load(df_path))
                # load subclass-specific logic
                loaded_obj._loadExtraData()
        return loaded_obj
    
    def __saveMetadata(self):
        """
        this function is used to store backend metadata for Axolotl library,
        don't inherit or modify the json file.
        """

        metadata_path = path.join(self.__folder_path, "_metadata.json")        
        with fopen(metadata_path, "w") as outfile:
            outfile.write(json.dumps(self.__metadata))


    ################# TO BE IMPLEMENTED BY SUBCLASSES #################


    @classmethod
    @abstractmethod
    def _dataDesc(cls) -> Dict:
        """
        describe here the structure of the app's database, i.e., the core set of
        AxlDF tables need to be created before the app can functions. Return a
        dictionary with key = table name and value = the appropriate AxlDF subclass
        """

        raise NotImplementedError("calling an unimplemented abstract method _dataDesc()")
        
    @abstractmethod
    def _creationFunc(self, *args, **kwargs):
        """
        override the *args and **kwargs with your own subclass-specific parameters.
        Using those parameters, perform all necessary operations to make sure all the
        database's tables (e.g., "keys") are set (via _setData()) and stored (via _saveData())
        accordingly. You can also setup additional attributes and store their serialized
        objects in the app's folder. Those will then need to be loaded back in the
        _loadExtraData() method.
        """

        raise Exception("calling an unimplemented abstract method _create()")

    @abstractmethod
    def _loadExtraData(self):
        """
        load back any serialized attribute from the _creationFunc().
        """

        raise Exception("calling an unimplemented abstract method _loadExtraData()")
