"""
parquet files

# by zhong wang @lbl.gov

This module provides:
1) data format coversion:
    dataframe -> parquet

TODO:


"""
import pyspark.sql.functions as F

def save_parquet(df, output_prefix, output_suffix, sort_col='', overwrite=True):
    """
    save dataframe as parquet, by default sorted to reduce file size and optimize loading
    
    """
    if sort_col and overwrite:
        df.sort(sort_col).write.mode("overwrite").parquet(output_prefix + output_suffix)
        return
    if overwrite:
        df.write.mode("overwrite").parquet(output_prefix + output_suffix)
        return  
    df.parquet(output_prefix + output_suffix)  