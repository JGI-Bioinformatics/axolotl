# axolotl utils
from pyspark.sql import SparkSession
from os import path
import re


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