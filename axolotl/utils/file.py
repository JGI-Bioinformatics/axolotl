# axolotl utils
from pyspark.sql import SparkSession
from os import path, makedirs
import re
import shutil
import gzip

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
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            return dbutils.fs.cp(matches_source["path"], matches_target["path"])
        except ImportError:
            raise Exception("can't access DataBricks DBUtils")
    else:
        raise NotImplementedError()
        
        
def make_dirs(file_path):
    matches = parse_path_type(file_path)
    
    if check_file_exists(file_path):
        raise Exception("file path exists!")
    
    if matches["type"] == "file":
        return makedirs(path.abspath(matches["path"]))
    elif matches["type"] == "dbfs":
        spark = SparkSession.getActiveSession()
        if spark == None:
            raise Exception("can't find any Spark active session!")        
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            return dbutils.fs.mkdirs("user/satria/test")
        except ImportError:
            raise Exception("can't access DataBricks DBUtils")
    else:
        raise NotImplementedError()


def fopen(file_path, mode="r"):
    matches = parse_path_type(file_path)
    
    if matches["type"] == "file":
        return open(matches["path"], mode)
    elif matches["type"] == "dbfs":
        return open(path.join("/dbfs", matches["path"]), mode)
    else:
        raise NotImplementedError()


def gunzip_deflate(input_zipped, output_path):
    matches_source = parse_path_type(input_zipped)
    matches_target = parse_path_type(output_path)
    
    # for now we will only handle same-type paths (e.g., local+local or dbfs+dbfs)
    if matches_source["type"] != matches_target["type"]:
        raise Exception("source_path and target_path needs to be the same type")
    
    if check_file_exists(output_path):
        raise Exception("target path exists!")
    
    if is_directory(input_zipped):
        raise Exception("source_path is a directory!")

    if matches_source["type"] == "file":
        with gzip.open(matches_source["path"], 'rb') as f_in:
            with open(matches_target["path"], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    elif matches_source["type"] == "dbfs":
        raise NotImplementedError()
    else:
        raise NotImplementedError()


def get_temp_dir(suffix=None, prefix=None, dir=None):
    spark = SparkSession.getActiveSession()
    if spark == None:
        raise Exception("can't find any Spark active session!")
    in_databricks = False
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        in_databricks = True
    except:
        pass

    class AxlTempDir:

        from random import randint
        from tempfile import mkdtemp
        from os import rmdir, path

        dir_path = None
        in_databricks = None
        def __init__(self, in_databricks, suffix=None, prefix=None, dir=None):
            if not in_databricks:
                self.dir_path = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
                self.in_databricks = False
            else:
                num_tries = 0
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
                self.in_databricks = True
                while not self.dir_path and num_tries < 5:
                    num_tries += 1
                    try:
                        dir_path = path.join(
                            dir or "dbfs:/tmp",
                            "{}{:016d}{}/".format(
                                prefix or "",
                                randint(1, (10**16) - 1),
                                suffix or ""
                            )
                        )
                        dbutils.fs.mkdirs(dir_path)
                        self.dir_path = dir_path
                        return
                    except:
                        pass
                raise Exception("failed to create dbfs temp dir")
        def __del__(self):
            pass
        def __enter__(self):
            return self.dir_path
        def __exit__(self, exc_type, exc_value, traceback):
            if not self.in_databricks:
                rmdir(self.dir_path)
            else:
                dbutils.fs.rm(self.dir_path, True)

    return AxlTempDir(in_databricks, suffix=suffix, prefix=prefix, dir=dir)