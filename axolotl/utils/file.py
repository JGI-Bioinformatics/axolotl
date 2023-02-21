# axolotl utils
from pyspark.sql import SparkSession
from os import path, makedirs
import re
import shutil


def parse_path_type(file_path):
    matches = re.match(
        "^((?P<type>[a-z]+):/{1,2}){0,1}(?P<path>[^#\\<\\>\\$\\+%!`&\\*'\\|\\{\\?\"=\\}:\\@]+)$",
        file_path
    )
    if matches == None:
        raise FileNotFoundError("can't recognize filepath {}".format(file_path))
    else:
        matches = matches.groupdict()
        
    if matches["type"] == None:
        matches["type"] = "file"

    return matches


def check_file_exists(file_path):
    matches = parse_path_type(file_path)
    
    if matches["type"] == "file":
        return path.exists(path.abspath(matches["path"]))
    elif matches["type"] == "dbfs":
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            try:
                dbutils.fs.ls(matches["path"])
                return True
            except:
                return False
        except ImportError:
            raise Exception("can't access DataBricks DBUtils")
    else:
        raise NotImplementedError()


def is_directory(file_path):
    matches = parse_path_type(file_path)
    
    if matches["type"] == "file":
        return path.isdir(path.abspath(matches["path"]))
    elif matches["type"] == "dbfs":
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            base_name = path.basename(matches["path"].rstrip("/"))
            for file in dbutils.fs.ls(matches["path"]):
                if file.name.rstrip("/") == base_name:
                    if file.name.endswith("/"):
                        return True
                    else:
                        return False
                else:
                    return True
            return True
        except ImportError:
            raise Exception("can't access DataBricks DBUtils")
    else:
        raise NotImplementedError()
        
        
def copy_file(source_path, target_path):
    matches_source = parse_path_type(source_path)
    matches_target = parse_path_type(target_path)
    
    # for now we will only handle same-type paths (e.g., local+local or dbfs+dbfs)
    if matches_source["type"] != matches_target["type"]:
        raise Exception("source_path and target_path needs to be the same type")
    
    if check_file_exists(target_path):
        raise Exception("target path exists!")
    
    if is_directory(source_path):
        raise Exception("source_path is a directory!")
    
    if matches_source["type"] == "file":
        shutil.copyfile(matches_source["path"], matches_target["path"])
    elif matches_source["type"] == "dbfs":
        raise NotImplementedError()
    else:
        raise NotImplementedError()
        
        
def make_dirs(file_path):
    matches = parse_path_type(file_path)
    
    if check_file_exists(file_path):
        raise Exception("file path exists!")
    
    if matches["type"] == "file":
        return makedirs(path.abspath(matches["path"]))
    elif matches["type"] == "dbfs":
        raise NotImplementedError()
    else:
        raise NotImplementedError()