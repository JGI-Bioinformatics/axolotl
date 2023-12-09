import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.sql.types import ArrayType, StringType

from axolotl.app.base import AxlApp
from axolotl.io.vcf import vcfDataIO, vcfMetaIO

from axolotl.data.vcf import vcfDF
from axolotl.data import MetaDF
from axolotl.utils.file import check_file_exists, is_directory, parse_path_type, get_temp_dir, make_dirs
from axolotl.utils.spark import get_spark_session_and_context

from itertools import chain
from typing import Dict
import os 
import pickle

class prs_calc_App(AxlApp):


  @classmethod
  def _dataDesc(cls) -> Dict:
    """
    Where AxlDFs are defined as a Dict. Format is "your_axlDF_name" : AxlDF_subclass
    This field only accepts AxlDFs and classes that inherit from AxlDF

    """

    return {
        "vcf_metadata": MetaDF,
        
    }


  def _creationFunc(self, vcf_metadata_df:MetaDF, vcf_vcf_df:vcfDF, gwas_df:DataFrame, dbsnp_vcf_df:vcfDF=None):
    """
    Args:
      vcf_metadata_df(MetaDF) : VCF Metadata 
      vcf_vcf_df(vcfDF) : vcfDF of your vcf data
      gwas_df(DataFrame) : Dataframe of GWAS data
      dbsnp_vcf_df(vcfDF) : dbsnp data
    
    Create function is used to create the app and also collect your inputs. It is also responsible for saving intermediates of your inputs into the app folder. 

    """
    spark, sc = get_spark_session_and_context()


    self._setData("vcf_metadata", vcf_metadata_df)
    self._saveData("vcf_metadata")
    current_vcf_metadata=vcf_metadata_df.df

    current_dbsnp_data = dbsnp_vcf_df
    if current_dbsnp_data != None:
      current_dbsnp_data = current_dbsnp_data.df
      current_dbsnp_data=self.update_chromosome(current_dbsnp_data)
      current_dbsnp_data = self.chop_dbsnp_rsID(current_dbsnp_data)
      current_dbsnp_data=self.add_dbsnp_code(current_dbsnp_data)
      
      dbsnp_pq_loc=self._folder_path
      dbsnp_pq_loc=os.path.join(dbsnp_pq_loc, "dbsnp_df_folder")
      current_dbsnp_data.write.parquet(dbsnp_pq_loc)

      self._dbsnp_df = spark.read.parquet(dbsnp_pq_loc)



      current_vcf_data = vcf_vcf_df.df
      missing_file_paths_list, full_sample_list = self.find_missing_samples_files(current_vcf_metadata)

      if missing_file_paths_list == "0": 
        current_vcf_data = self.make_samples_df(current_vcf_metadata,current_vcf_data)
      elif missing_file_paths_list == "-1":
        print("There is a missmatch in samples")
        current_vcf_data = self.make_samples_df(current_vcf_metadata,current_vcf_data)
      else: 
        current_vcf_data = self.make_samples_df_fill_missing_samples(missing_file_paths_list, full_sample_list, current_vcf_metadata, current_vcf_data)
      
      current_vcf_data = self.update_rsID(current_dbsnp_data,current_vcf_data)


      vcf_loc=self._folder_path
      vcf_loc=os.path.join(vcf_loc, "vcf_df_folder")
      current_vcf_data.write.parquet(vcf_loc)

      self._vcf_df = spark.read.parquet(vcf_loc)



      current_gwas_df = self.gwas_fill_rsID(current_dbsnp_data, gwas_df)
      code_mapping = self.allele_encoding(current_dbsnp_data, current_vcf_data, code_mappings='')
      current_gwas_df = self.gwas_add_code(current_dbsnp_data,current_gwas_df,code_mapping, output='')
    
      gwas_loc=self._folder_path
      gwas_loc=os.path.join(gwas_loc, "gwas_df_folder")
      current_gwas_df.write.parquet(gwas_loc)

      self._gwas_df = spark.read.parquet(gwas_loc)
  

    


  def _loadExtraData(self):
    """
    This is here to load the Dataframes you create in the create function. We used _dataDesc to hold all AxlDFs. This is only stuff you need initially at the start of your pipeline that don't already have a function that takes care of reading in the data. In our case Calc_PRS. 
    """
    spark, sc = get_spark_session_and_context()

    app_loc=self._folder_path

    gwas_loc=os.path.join(app_loc, "gwas_df_folder")
    self._gwas_df = spark.read.parquet(gwas_loc)

    dbsnp_loc=os.path.join(app_loc, "dbsnp_df_folder")
    self._dbsnp_df = spark.read.parquet(dbsnp_loc)
    
    vcf_loc=os.path.join(app_loc, "vcf_df_folder")
    self._vcf_df = spark.read.parquet(vcf_loc)

    return



  def update_chromosome(self,vcf_df): 
    """
    Input:Dataframe with a column called "chromosome"
    Method will set chromosome to old_chromosome. 
    Will create new "chromosome" column after extracting the chromosome number 
    Ex: NC_000007.13 -> 7 or NC_000008.10 -> 8

    """
    # table to replace chromosome numbering. Used for update_chromosome function.
    mapping = {
    'X': 23,
    'Y': 24,
    'M': 25
    }
    mapping.update({str(i+1):(i+1) for i in range(25)})
    chr_mapping = F.create_map([F.lit(x) for x in chain(*mapping.items())])

    
    new_vcf_df=vcf_df.withColumnRenamed("chromosome", "old_chromosome").withColumn("chromosome", F.regexp_extract(F.col("old_chromosome"), r'NC_0+(\d+).|(?:1\d?|2[0-5]?)', 1))
    new_vcf_df=new_vcf_df.withColumn('chromosome', chr_mapping[new_vcf_df['chromosome']])
    return new_vcf_df
  


  def chop_dbsnp_rsID(self,dbsnp_df):
    """
    Transforms the "ids" column in the given DataFrame by extracting the numerical part
    from entries in the format "rs<digits>". Additionally, casts the "ids" column to long datatype.
    Entries not matching the "rs<digits>" format remain unchanged.
    """
    if "ids" in dbsnp_df.columns:
        new_dbsnp_df = (dbsnp_df
                       .withColumn("ids",F.when(F.regexp_extract(F.col("ids"), r'^rs(\d+)$', 1) != "", F.regexp_extract(F.col("ids"), r'^rs(\d+)$', 1))
                                   .otherwise(F.col("ids")))
                       .withColumn("ids", F.col("ids").cast("long"))
                       )
        return new_dbsnp_df
        


  def add_dbsnp_code(self,dbsnp_df):
    """
    Augments the given DataFrame by exploding the combined 'references' and 'alts' columns 
    into two new columns: 'code' (position/index) and 'allele' (actual value). 
    The original values are concatenated and then split to achieve the explosion.
    Ex:
    Input 
    +---+-----------+------+
    | id| references| alts |
    +---+-----------+------+
    |  1|          A|   G,T|
    |  2|          C|     A|
    +---+-----------+------+
    Output
    +---+-----+------+
    | id| code|allele|
    +---+-----+------+
    |  1|    0|     A|
    |  1|    1|     G|
    |  1|    2|     T|
    |  2|    0|     C|
    |  2|    1|     A|
    +---+-----+------+
    """
    new_dbsnp_df = dbsnp_df
    new_dbsnp_df = dbsnp_df.select("*",F.posexplode(F.split(F.concat_ws(',', 'references', 'alts'), ',')).alias('code', 'allele'))
    return new_dbsnp_df



  def make_samples_df(self, metadata_df, vcf_df):
    """
    Extracts sample names from the 'metadata_df' and creates new columns in 'vcf_df' 
    for each sample. Each new column is populated with values extracted from the 
    "samples" column of 'vcf_df' based on the index of the sample name.
    """
    sample_names=metadata_df.filter(metadata_df.key == "samples").select("value").collect()[0][0]
    sample_names=sample_names.split("\t")
    new_vcf_df = vcf_df
    for idx, sample_name in enumerate(sample_names):
      new_vcf_df= new_vcf_df.withColumn(sample_name, F.col("samples").getItem(idx))
    return new_vcf_df
  


  def find_missing_samples_files(self,metadata_df):
    filtered_metadata_df = metadata_df.filter(metadata_df.key == "samples")

    # Convert the 'value' column to an array of values
    df_with_array = filtered_metadata_df.withColumn('array', F.split(F.col('value'), '\t'))

    # Find the length of each array and the maximum length
    df_with_length = df_with_array.withColumn('length', F.size('array'))
    max_length = df_with_length.agg(F.max('length')).collect()[0][0]

    # Filter out rows where the array length is less than the max length
    df_filtered = df_with_length.filter(df_with_length['length'] < max_length)

    # Filter the largest set
    largest_set_df = df_with_length.filter(df_with_length['length'] == max_length)
    largest_set = largest_set_df.select('array').first()['array']
    
    if len(df_filtered.head(1)) == 0:
      #this represents the situation where all files have the same number samples
      return ["0"] , [largest_set]
    
    else:
      # Explode the array into multiple rows
      df_exploded = df_filtered.withColumn('element', F.explode('array'))

      # Find distinct elements in all smaller arrays
      distinct_elements = df_exploded.select('element').distinct().rdd.map(lambda r: r[0]).collect()

      # Check which elements from the distinct list are not in the largest set
      missing_elements = set(distinct_elements) - set(largest_set)


      # If there are missing elements, find the file paths that contain them
      if len(missing_elements)==0:
          file_paths_df = df_exploded.filter(df_exploded['length']<max_length).select('file_path').distinct()
          file_paths = file_paths_df.rdd.map(lambda r: r[0]).collect()
          return file_paths, largest_set
      else:
          #This represents the situation where there is a missmatch of samples
          return ["-1"], ["-1"]
  


  def make_samples_df_fill_missing_samples(self, missing_file_path_list, full_sample_names_list, metadata_df, vcf_df):
    new_vcf_df = vcf_df
    # Broadcast the small DataFrame
    broadcasted_metadata = F.broadcast(metadata_df)

    # Create a dictionary for missing file paths
    file_dict_df = broadcasted_metadata.filter(
        (metadata_df.key == "samples") & (metadata_df.file_path.isin(missing_file_path_list))
    )
    missing_file_path_dict = {row["file_path"]: row["value"].split("\t") for row in file_dict_df.collect()}

    # Filter out the missing and existing dataframes
    missing_df = new_vcf_df.filter(new_vcf_df.file_path.isin(missing_file_path_list))
    full_df = new_vcf_df.filter(~new_vcf_df.file_path.isin(missing_file_path_list))

    # Add columns to the full dataframe
    full_df = full_df.select("*", *[F.expr(f"samples[{i}] as {name}") for i, name in enumerate(full_sample_names_list)])

    # Process missing file paths
    for missing_file_path, sub_sample_names in missing_file_path_dict.items():
        sub_df = missing_df.filter(missing_df.file_path == missing_file_path)
        sub_df = sub_df.select("*", *[F.expr(f"samples[{i}] as {name}") for i, name in enumerate(sub_sample_names)])
        full_df = full_df.unionByName(sub_df, allowMissingColumns=True)

    return full_df.fillna("0")


  def update_rsID(self, dbsnp_df, in_vcf_df, out_vcf_df='', keep=False):
    """_update vcf_df to latest rsIDs based on dbSNP using chrom and position, using assembly-specific dbsnp. The original column 'ID" will be renamed to 'oldID'._

    Args:
        dbsnp (vcfSet): _dbSNP vcfSet
        in_vcf (vcfSet): _input vcfSet
        out_vcf_df (_str_): _output vcf in parquet, if empty, return vcfSet
        keep (_bool_): _whether or not to keep records in gVCF that have no matching rsIDs in dbSNP, default to False, no keeping
    """
    # update column names in dbSNP to match gVCF
    filtered_dbsnp_df = (dbsnp_df
                         .select(F.col('chromosome'), F.col('position'), F.col('ids'))
                         )

    # Get the list of all columns from the original DataFrame
    #all_columns = in_vcf.get("data").df.columns
    #join_columns = ['chromosome', 'position']

    new_vcf_df=in_vcf_df
    if keep: # keep records in gVCF that have no matching rsIDs
        new_vcf_df = (in_vcf_df
                        .withColumnRenamed('ids', 'oldID')
                        .join(filtered_dbsnp_df, on=['chromosome', 'position'], how='left')
                        ) 
    else:   
        new_vcf_df = (in_vcf_df
                        .withColumnRenamed('ids', 'oldID')
                        .join(filtered_dbsnp_df, on=['chromosome', 'position'], how='inner')
                        )

    if out_vcf_df != '':
      in_vcf_df.write.mode('overwrite').parquet(out_vcf_df)
    
    return new_vcf_df



  def gvcf_to_vsf(self,gvcf_df, metadata_df, output='', min_quality=0, qfilter=False, code_mapping=''):
    """_convert gVCF to sparse gVSF format_
      
    Args:
        gvcf_vcf (vcfSet): _gVCF_
        vsf (str, optional): _output gVSF_. If empty, return the dataframe(Default).
        min_quality (int, optional): _filter out low quality variant below this_. Defaults to 0.
        qfilter (bool, optional): _whether or not apply a filter for variant with a'PASS' status. Defaults, False.
        code_mapping (str, optional): _code mapping file to transform allele coding_. Defaults to ''.

    Returns:
        _any_: _none or dataframe_

    Example Output:
    +------+---------+----+----+-----------+
    |sample|     rsID|code|dose|sample_name|
    +------+---------+----+----+-----------+
    |     3|  2249382|   1| 2.0|    Sample1|
    |     6| 58616815|   1| 2.0|    Sample2|
    |     7|150848999|   1| 2.0|    Sample2|
    +------+---------+----+----+-----------+

    """
    spark, sc = get_spark_session_and_context()

    vsf_folder = os.path.join(
            self._folder_path, "vsf",
            "{}-{}".format(
                min_quality,
                qfilter
            )
        )
    if not check_file_exists(vsf_folder):

      # code is derived from gvcf
      samples = metadata_df.filter(metadata_df.key == "samples").select("value").collect()[0][0]
      samples = samples.split("\t")
      #names = {i:i for i in range(len(samples))}
      names = {i:samples[i] for i in range(len(samples))}


      # filter the data
      if min_quality > 0:
          #gvcf = gvcf.where(F.col('QUAL') >= min_quality)
          gvcf = gvcf_df.where(F.col('qual') >= min_quality)
      if qfilter:
          #gvcf = gvcf.where(F.col('FILTER') == 'PASS')
          gvcf = gvcf_df.where(F.col('filter') == 'PASS')    
      
      # split the two alleles
      a1 = (gvcf_df
              .select(
              'ids',
              *((F.split(F.split(c, ':').getItem(0), '[|/]').getItem(0).astype('int').alias(c) for c in samples))
              )
              .fillna(0) # assume it is reference allele if missing
              .withColumn('gts', F.array(*samples))
              .select('ids', 'gts')
          )

      a2 = (gvcf_df
              .select(
              'ids',
              *((F.split(F.split(c, ':').getItem(0), '[|/]').getItem(1).astype('int').alias(c) for c in samples))
              )
              .fillna(0)
              .withColumn('gts', F.array(*samples))
              .select('ids', 'gts')
          )
      # this takes a long while
      # convert genotype data to coordinated matrix
      a1 = IndexedRowMatrix(a1.rdd.map(lambda row: IndexedRow(*row))).toCoordinateMatrix()
      a2 = IndexedRowMatrix(a2.rdd.map(lambda row: IndexedRow(*row))).toCoordinateMatrix()

      a1 = (spark
      .createDataFrame(a1.transpose().entries)
      .where(F.col('value')>0)
      .toDF('sample', 'rsID', 'code')
      )
      a2 = (spark
      .createDataFrame(a2.transpose().entries)
      .where(F.col('value')>0)
      .toDF('sample', 'rsID', 'code')
      )

      # create the genotype sparse matrix
      # rows are samples
      # columns are genotype (rsID:gt)
      # values are doses (how many alleles)
      gt = a1.union(a2)
      gt = (gt
              .withColumn('dose', F.lit(1.0))
              .select('sample', 'rsID', F.col('code').astype('int'), 'dose')
              .groupby('sample', 'rsID', 'code')
              .agg(F.sum('dose').alias('dose'))
          )
      
      
      if code_mapping != '':
          gt = (gt
                  .withColumnRenamed('code', 'code1k')
                  .join(code_mapping.select(F.col('ids').alias('rsID'), 'code', 'code1k'), on=['rsID', 'code1k'], how='left')
                  .drop('code1k')
                  .fillna({'code':0})
          )
          
      # after the conversion, sample becomes index to the original samples, let's add the name back
      sample_mapping = F.create_map([F.lit(x) for x in chain(*names.items())])
      gt = gt.withColumn('sample_name', sample_mapping[gt['sample']] ) 
          
      if output != '':
          #gt.write.mode('overwrite').parquet(output)
          gt.to_pandas_on_spark().to_pandas().to_csv(output, index=False, header=True, sep='\t')
      
      gt.write.mode('overwrite').parquet(vsf_folder)

    
    return spark.read.parquet(vsf_folder)



  def gwas_fill_rsID(self,dbsnp_df, gwas_df, output='', overwrite=False):
      """_update gwas rsID with those found in dbSNP_

      Args:
          dbsnp (vcf_df): dbsnp object that is in vcf_set format 
          ref (pyspark df): gwas reference
          out_file (str, optional): _description_. Defaults to ''.
          overwrite (bool, optional): _description_. Defaults to False.

      Returns:
          _type_: _description_
      """

      ref = (gwas_df
            .join(dbsnp_df.select('chromosome', 'position', F.col('ids').alias('newids')), on=['chromosome', 'position'], how='left')
      )
      if overwrite:
          ref = ref.drop('ids').withColumnRenamed('newids', 'ids')     
      else:
          ref = (ref
            .withColumn('ids', F.when(F.isnull('ids'), F.col('newids')).otherwise(F.col('ids')))
            .drop('newids')
            )               
      ref = ref.drop_duplicates(['trait', 'ids', 'alt'])
      if output == '':
          return ref
      ref.write.mode('overwrite').parquet(output)



  def allele_encoding(self, dbsnp_df, gvcf_df, code_mappings=''):
    """_map alleles codes in gGVCF to dbSNP codes_
    The result is a mapping from gGVF code('code1k') to dbSNP code ('code'), which later will be used to transform codes back and forth.
    
    Args:
        dbsnp (_dataframe_): _dbSNP dataframe_
        in_gvcf (_dataframe_): _input gVCF dataframe_
        code_mappings (_any_): _file name to store the map from gVCF allele to dbSNP allele, if empty, return the dataframe_
    """

    code_df = (gvcf_df
               .select( F.col('ids').astype('long'),
                       F.posexplode(F.split(F.concat_ws(',', 'references', 'alts'), ',')).alias('code1k', 'allele')
                       )
               .select('ids', F.col('code1k').astype('int'), 'allele')
               .join(dbsnp_df.select(F.col('ids'), F.col('code').astype('int'), 'allele'), on=['ids', 'allele'])
               .select('ids', 'code1k', 'code')
               )
    if code_mappings == '':
        return code_df
    code_df.write.mode('overwrite').parquet(code_mappings)



  def gwas_add_code(self,dbsnp_df, gwas_df, code_mapping, output=''):
    """_summary_

    Args:
        dbsnp (_type_): _description_
        ref (_type_): _description_
        code_mapping (_type_): _description_
        out_file (str, optional): _description_. Defaults to ''.

    Returns:
        _type_: _description_
    """

    gwas_df = ( gwas_df
               .join(dbsnp_df.select('ids', F.col('allele').alias('alt'),'code'), on=['ids', 'alt'], how='left')
               .na.drop() # drop alts that are not in dbSNP
               )
    
    if code_mapping != '':
      # mapping ref codes to 1kgenome codes
      gwas_df = (gwas_df
                 .join(code_mapping.select(F.col('ids').alias('ids'), 'code', 'code1k'), on=['ids', 'code'], how='left')
                 .fillna({'code1k':0})
                 .drop('code')
                 .withColumnRenamed('code1k', 'code')
                 .distinct()
                 )
      
    if output == '':
      return gwas_df
    gwas_df.write.mode('overwrite').parquet(output)      



  def calc_PRS(self,gvsf, gwas_df,min_quality, qfilter, output=''):
    """
    Method for calculating Polygeneic Risk Scores (PRS). Options min_quality and qfilter need to correspond to the values in gvcf_to_vsf. They are mainly used for retrieving the path to the results folder.  
    """

    results_folder = os.path.join(
            self._folder_path, "results",
            "{}-{}".format(
                min_quality,
                qfilter
            )
        )
    if not check_file_exists(results_folder):
      gwas_df = gwas_df.withColumn('index', F.concat_ws('|', 'trait', 'pubmedID'))
      results = (gvsf
                .join(gwas_df.select("*",F.column("ids").alias("rsID")), on=['rsID', 'code'])
                .withColumn('OR', F.col('dose')*F.col('OR'))
                .groupby(['sample_name', 'index'])
                .agg(F.sum('OR').alias('prs_score'))
      )
      # add trait info
      results = ( results
                .join(gwas_df.select('index', 'trait', 'pubmedID', 'date'), on='index', how='left')
                .drop('index')
                .distinct()
                .sort(['sample_name', 'trait']) 
      )
      results.write.parquet(results_folder)

      if output != '':
        results.to_pandas_on_spark().to_pandas().to_csv(output, index=False, header=True, sep='\t')
    
    spark, sc = get_spark_session_and_context()
    return spark.read.parquet(results_folder)



  def calc_prs_pipeline(self, input_min_quality=0, input_qfilter=False):
    """
    Runs whole pipeline. 
    """

    # fetch current 
    current_vcf_metadata = self._getData("vcf_metadata").df
    current_dbsnp_data = self._dbsnp_df
    current_gwas_df = self._gwas_df
    current_vcf_data = self._vcf_df
    


    gvsf = self.gvcf_to_vsf(current_vcf_data,current_vcf_metadata, output='', min_quality=input_min_quality, qfilter=input_qfilter, code_mapping='')


    results=self.calc_PRS(gvsf=gvsf,gwas_df=current_gwas_df,min_quality=input_min_quality, qfilter=input_qfilter)

    return results