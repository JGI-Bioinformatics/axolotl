"""
parquet format conversion tools 

# by zhong wang @lbl.gov

This module provides:
1) data format coversion:
    dataframe -> parquet

TODO:
Add support from seq -> fastq
Add support from seq -> fasta

"""
import pyspark.sql.functions as F

def save_parquet(df, output_file, sort_col='', overwrite=True):
    """
    save dataframe as parquet, by default sorted to reduce file size and optimize loading
    
    """
    if sort_col and overwrite:
        df.sort(sort_col).write.mode("overwrite").parquet(output_file)
        return
    if overwrite:
        df.write.mode("overwrite").parquet(output_file)
        return  
    df.parquet(output_file)  