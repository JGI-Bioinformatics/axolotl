from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# preprocess gwas files


def gwas_csv_input(gwas, output=''):
  """_convert gwas result file to parquet_

  Args:
      gwas (_string_): _gwas input file in csv format_
      output (_string_): _output file_, if empty, return the dataframe
  """

  #make a mapping dictionary
  mapping = {
  'X': 23,
  'Y': 24,
  'M': 25
  }
  mapping.update({str(i+1):(i+1) for i in range(25)})
  chr_mapping = F.create_map([F.lit(x) for x in chain(*mapping.items())])


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