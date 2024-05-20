import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def find_median_of_duplicates(df: DataFrame, col_name: str, list_of_cols: list, nonNA_list_of_cols:list) -> DataFrame:

    """
    Filters the DataFrame to remove rows with null values in specified columns, computes the median of a specified column 
    grouped by other columns, and updates the original DataFrame to replace the column values with the median values where 
    they exist. It also ensures that only one row per group is retained, based on the order of the column of interest.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - col_name (str): The name of the column for which the median needs to be computed. Also used for updating values to median.
    - list_of_cols (list): A list of column names used for grouping the DataFrame.
    - nonNA_list_of_cols (list): A list of column names that must not contain NA values for the row to be included.

    Returns:
    - DataFrame: A DataFrame with updated values where the specified column's values are replaced by their median for each group
                 defined by 'list_of_cols', with no duplicate rows for these groups and no NA values in 'nonNA_list_of_cols'.

    The method first ensures that all rows in the DataFrame do not have NA values in the 'col_name' and all columns specified
    in 'nonNA_list_of_cols'. It then groups the data by 'list_of_cols' and calculates the median of 'col_name'. These median 
    values are joined back to the original DataFrame. The 'col_name' in each row is updated to this median if it exists. 
    Finally, it filters out duplicate rows, keeping only the first row of each group sorted by 'col_name'. It also ensures that 
    there are no NA values in the columns specified by 'nonNA_list_of_cols'.
    """

    # Filter out rows where the column of interest is null
    df_non_null = df.filter(F.col(col_name).isNotNull())

    for col_name in nonNA_list_of_cols:
        df_non_null = df_non_null.filter(F.col(col_name).isNotNull())
    
    # Group by the specified columns and calculate the median
    median_df = df_non_null.groupBy(list_of_cols).agg(
        F.expr(f'percentile_approx({col_name}, 0.5)').alias('median')
    )

    # Join the original DataFrame with the median values
    result_df = df.join(median_df, on=list_of_cols, how='left')

    # Ensure we captured the correct row
    result_df = result_df.filter(F.col(col_name) == F.col('median'))

    
    # Replace the column values with the median where it is not null
    result_df = result_df.withColumn(
        col_name,
        F.when(F.col('median').isNotNull(), F.col('median')).otherwise(F.col(col_name))
    ).drop('median')

    
    # Remove duplicates, keeping only one row per group
    window_spec = Window.partitionBy(list_of_cols).orderBy(F.col(col_name))
    result_df = result_df.withColumn("row_num", F.row_number().over(window_spec))
    result_df = result_df.filter(F.col("row_num") == 1).drop("row_num")
    
    for col_name in nonNA_list_of_cols:
        result_df = result_df.filter(F.col(col_name).isNotNull())
    
    return result_df
