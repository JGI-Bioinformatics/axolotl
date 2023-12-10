from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from axolotl.utils.spark import get_spark_session_and_context

# preprocess gwas files


def gwas_csv_input(gwas, file_type="csv", output=''):
  """_extract data from gwas file and output result file to parquet_

  Args:
      gwas (_string_): _gwas input file in csv format_
      file_type (_string_): accepts json, pq or parquet, and csv
      output (_string_): _output file_, if empty, return the dataframe
  """

  spark,sc = get_spark_session_and_context()

  #make a mapping dictionary
  mapping = {
  'X': 23,
  'Y': 24,
  'M': 25
  }
  mapping.update({str(i+1):(i+1) for i in range(25)})
  chr_mapping = F.create_map([F.lit(x) for x in chain(*mapping.items())])

  if file_type=="json" : 
    ref = (spark
          .read
          .json(gwas)
          .withColumn('CHR_ID', F.when(F.isnull('CHR_ID'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 1)).otherwise(F.col('CHR_ID')))
          .withColumn('CHR_POS', F.when(F.isnull('CHR_POS'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 2)).otherwise(F.col('CHR_POS')))       
          .select(F.col('SNP_ID_CURRENT').astype('long').alias('ids'), 
                  F.col('rallele').alias('alt'),
                  F.col('OR or BETA').astype('float').alias('OR'),
                  F.col('CHR_ID').alias('chromosome'),
                  F.col('CHR_POS').astype('long').alias('position'),
                  F.col('DISEASE/TRAIT').alias('trait'),
                  F.col('DATE ADDED TO CATALOG').astype('date').alias('date'),
                  F.col('PUBMEDID').astype('long').alias('pubmedID')
          ).where(
            (F.col('OR')>0.0)
          )
    )

  elif ((file_type=="pq") | (file_type=="parquet")) : 
    ref = (spark
          .read
          .parquet(gwas)
          .withColumn('CHR_ID', F.when(F.isnull('CHR_ID'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 1)).otherwise(F.col('CHR_ID')))
          .withColumn('CHR_POS', F.when(F.isnull('CHR_POS'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 2)).otherwise(F.col('CHR_POS')))       
          .select(F.col('SNP_ID_CURRENT').astype('long').alias('ids'), 
                  F.col('rallele').alias('alt'),
                  F.col('OR or BETA').astype('float').alias('OR'),
                  F.col('CHR_ID').alias('chromosome'),
                  F.col('CHR_POS').astype('long').alias('position'),
                  F.col('DISEASE/TRAIT').alias('trait'),
                  F.col('DATE ADDED TO CATALOG').astype('date').alias('date'),
                  F.col('PUBMEDID').astype('long').alias('pubmedID')
          ).where(
            (F.col('OR')>0.0)
          )
    )
      
  else: 
    ref = (spark
          .read
          .csv(gwas, sep='\t', header=True)
          .withColumn('CHR_ID', F.when(F.isnull('CHR_ID'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 1)).otherwise(F.col('CHR_ID')))
          .withColumn('CHR_POS', F.when(F.isnull('CHR_POS'), F.regexp_extract('SNPs', r'chr([\dXYM]+):(\d+)', 2)).otherwise(F.col('CHR_POS')))       
          .select(F.col('SNP_ID_CURRENT').astype('long').alias('ids'), 
                  F.col('rallele').alias('alt'),
                  F.col('OR or BETA').astype('float').alias('OR'),
                  F.col('CHR_ID').alias('chromosome'),
                  F.col('CHR_POS').astype('long').alias('position'),
                  F.col('DISEASE/TRAIT').alias('trait'),
                  F.col('DATE ADDED TO CATALOG').astype('date').alias('date'),
                  F.col('PUBMEDID').astype('long').alias('pubmedID')
          ).where(
            (F.col('OR')>0.0)
          )
    )
    
  ref = ref.withColumn('chromosome', chr_mapping[ref['chromosome']])

  if output == '':
        return ref
  (ref
  .write
  .mode('overwrite')
  .parquet(output)
  )
