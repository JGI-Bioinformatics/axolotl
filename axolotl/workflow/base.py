from abc import abstractmethod
from axolotl.core import AxlDF
from axolotl.utils.file import check_file_exists, make_dirs, fopen
from typing import Dict
import json
from os import path


class AxlWorkFlow:

    def __init__(self, wf_folder_path: str, metadata: Dict):
        self.__data = {}
        self.__folder_path = wf_folder_path
        self.__metadata = metadata

    @classmethod
    def getOrCreate(cls, wf_folder_path: str, *args, **kwargs):
        wf_object = cls.__loadFolder(wf_folder_path)
        if not wf_object:
            print("can't load  from folder, creating a new object")
            wf_object = cls.__createFolder(wf_folder_path, *args, **kwargs)        
        if not wf_object:
            raise Exception("failed to load or create object")
        return wf_object
        
    def _getData(self, key: str):
        return self.__data[key]

    def _setData(self, key: str, df: AxlDF):
        self.__data[key] = df

    def _saveData(self, key: str, overwrite:bool=False):
        self.__data[key].store(path.join(self.__folder_path, "data", key), overwrite=overwrite)

    @classmethod
    def __createFolder(cls, wf_folder_path: str, *args, **kwargs):
        # create a new empty data dir structure, along with metadata
        if check_file_exists(wf_folder_path):
            raise Exception("Trying to create a new AxlWF folder but folder exists! {}".format(wf_folder_path))
        created_obj = cls(wf_folder_path, {
            "class_name": cls.__name__
        })
        make_dirs(wf_folder_path)
        make_dirs(path.join(wf_folder_path, "data"))
        make_dirs(path.join(wf_folder_path, "extra_data"))
        created_obj.__saveMetadata()
        # implement subclass-specific logic
        created_obj._create(*args, **kwargs)
        return created_obj

    @classmethod
    def __loadFolder(cls, wf_folder_path: str):
        loaded_obj = None
        if check_file_exists(wf_folder_path): # check if folder exists
            # check metadata and load __data variable
            metadata_path = path.join(wf_folder_path, "_metadata.json")
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
                loaded_obj = cls(wf_folder_path, metadata)
                data_folder = path.join(wf_folder_path, "data")
                for key, data_class in cls._dataDesc().items():
                    df_path = path.join(data_folder, key)
                    if not check_file_exists(df_path):
                        raise FileNotFoundError("can't find data folder {}!".format(key))
                    loaded_obj._setData(key, data_class.load(df_path))
                loaded_obj._load_extra_data()
        return loaded_obj
    
    def __saveMetadata(self):
        metadata_path = path.join(self.__folder_path, "_metadata.json")        
        with fopen(metadata_path, "w") as outfile:
            outfile.write(json.dumps(self.__metadata))


    ################# TO BE IMPLEMENTED BY SUBCLASSES #################

    @classmethod
    @abstractmethod
    def _dataDesc(cls) -> Dict:
        raise NotImplementedError("calling an unimplemented abstract method _dataDesc()")
        
    @abstractmethod
    def _load_extra_data(self):
        raise Exception("calling an unimplemented abstract method _load_extra_data()")

    @abstractmethod
    def _create(self, *args, **kwargs):
        raise Exception("calling an unimplemented abstract method _create()")
