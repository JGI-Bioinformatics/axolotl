"""
PySPark implementation of weighted LPA

"""

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import Window

from pyspark.sql import DataFrame

def run_iteration(df: DataFrame)-> DataFrame:
  df2 = make_clusters(df)
  newdf = (df
           .join(df2, df.src == df2.node_id, how='left')
          )
  newdf = (newdf
           .withColumn("filled_cid", F.when(F.col("new_cid") >=0, F.col("new_cid")).otherwise(F.col("cid"))) 
           .drop("new_cid")
           .withColumnRenamed("filled_cid", "new_cid")
           .withColumn("new_changed", F.col("cid") != F.col("new_cid"))
          )
  if 'weight' not in df.columns:
      return (newdf.select("src", "dst", "new_cid", "new_changed")
                .withColumnRenamed("new_cid", "cid")
                .withColumnRenamed("new_changed", "changed")
      )
  else:
      return (newdf
              .select("src", "dst", "weight", "new_cid", "new_changed")
              .withColumnRenamed("new_cid", "cid")
              .withColumnRenamed("new_changed", "changed")
             )

def make_clusters(df: DataFrame)-> DataFrame:
  if 'weight' in df.columns:
    cnts = (df
            .groupBy("dst", "cid")
            .agg(F.sum("weight").alias("sumweight"))
           )
  else:
    cnts = (df
            .groupBy("dst", "cid")
            .agg(F.count("changed").alias("sumweight"))
           )
  w = (Window
     .partitionBy("dst")
     .orderBy(F.col("sumweight").desc(), F.col("cid"))
    )

  return (cnts
            .withColumn("rn", F.row_number().over(w))
            .where(F.col("rn") == 1)
            .select("dst", "cid")
            .withColumnRenamed("dst", "node_id")
            .withColumnRenamed("cid", "new_cid")
           )

def run_wlpa(df: DataFrame, maxIter=5)-> DataFrame:
    """
    df is an edge_df dataframe from edgegen
    it should have src, dst, and weight (optional)
    """

    df= (df
     .withColumn("cid", F.col("src"))
     .withColumn("changed", F.lit(True))
    ).checkpoint(eager=True)

    for i in range(maxIter):
        df = run_iteration(df).checkpoint(eager=True)
        cnt = df.agg(F.sum(F.col("changed").cast("long")).alias('total_changes')).first()['total_changes']
        if cnt == 0:
            print("Stop at iteration %d" % i) 
            break

    return make_clusters(df).withColumnRenamed('node_id', 'id').withColumnRenamed('new_cid', 'label')