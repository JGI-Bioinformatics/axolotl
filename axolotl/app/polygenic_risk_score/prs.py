import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

from axolotl.app.base import AxlApp
from axolotl.io.vcf import vcfDataIO, vcfMetaIO

from axolotl.data.vcf import vcfDF
from axolotl.data import MetaDF
from axolotl.utils.file import check_file_exists, is_directory, parse_path_type, get_temp_dir, make_dirs
from axolotl.utils.spark import get_spark_session_and_context
from axolotl.utils.deduplicate_find_median import find_median_of_duplicates

from itertools import chain
from typing import Dict
import os 
import pickle

class prs_calc_App(AxlApp):


  @classmethod
  def _dataDesc(cls) -> Dict:
    """
    Returns metadata as a Dict. Format is "axlDF_name" : AxlDF_subclass
    This field only accepts AxlDFs and classes that are inherited from AxlDF

    """

    return {
        "vcf_metadata": MetaDF,
        
    }


  def _creationFunc(self, vcf_metadata_df:MetaDF, vcf_vcf_df:vcfDF, gwas_df:DataFrame, dbsnp_vcf_df:vcfDF=None, vcf_deduplicate:bool=True, dbsnp_deduplicate:bool=True, gwas_deduplicate:bool=True):
    """
    Creates the PRS App and saves intermediate data into the app folder.
    Args:
      vcf_metadata_df(MetaDF) : metadata dataframe 
      vcf_vcf_df(vcfDF) : genome VCF dataframe
      gwas_df(DataFrame) : Dataframe of GWAS dataframe
      dbsnp_vcf_df(vcfDF) : dbsnp VCF dataframe

    """
    spark, sc = get_spark_session_and_context()


    self._setData("vcf_metadata", vcf_metadata_df)
    self._saveData("vcf_metadata")
    current_vcf_metadata=vcf_metadata_df.df

      
    current_dbsnp_data = dbsnp_vcf_df
    if current_dbsnp_data != None:
      current_dbsnp_data = current_dbsnp_data.df
        
      if dbsnp_deduplicate==True:
        current_dbsnp_data = current_dbsnp_data.dropDuplicates(['ids','chromosome','position'])
          
      current_dbsnp_data=self.update_chromosome(current_dbsnp_data)
      current_dbsnp_data = self.chop_dbsnp_rsID(current_dbsnp_data)
      current_dbsnp_data=self.add_dbsnp_code(current_dbsnp_data)
      dbsnp_pq_loc=self._folder_path
      dbsnp_pq_loc=os.path.join(dbsnp_pq_loc, "dbsnp_df_folder")
      current_dbsnp_data.write.parquet(dbsnp_pq_loc)

      self._dbsnp_df = spark.read.parquet(dbsnp_pq_loc)



      current_vcf_data = vcf_vcf_df.df

      if vcf_deduplicate==True:
        current_vcf_data = current_vcf_data.dropDuplicates(['ids','chromosome','position','references','alts'])
      missing_file_paths_list, full_sample_list = self.find_missing_samples_files(current_vcf_metadata)

      if missing_file_paths_list == ['0']: 
        current_vcf_data = self.make_samples_df(current_vcf_metadata,current_vcf_data)
      elif missing_file_paths_list == ['-1']:
        print("There is a missmatch in samples")
        current_vcf_data = self.make_samples_df(current_vcf_metadata,current_vcf_data)
      else: 
        current_vcf_data = self.make_samples_df_fill_missing_samples(missing_file_paths_list, full_sample_list, current_vcf_metadata, current_vcf_data)
      
      current_vcf_data = self.update_rsID(current_dbsnp_data,current_vcf_data)


      vcf_loc=self._folder_path
      vcf_loc=os.path.join(vcf_loc, "vcf_df_folder")
      current_vcf_data.write.parquet(vcf_loc)

      self._vcf_df = spark.read.parquet(vcf_loc)


      if gwas_deduplicate==True:
        gwas_df = find_median_of_duplicates(gwas_df,'OR',['ids','alt','chromosome','position','trait','pubmedID'],['ids','alt','OR','chromosome','position'])
      current_gwas_df = self.gwas_fill_rsID(current_dbsnp_data, gwas_df)
      code_mapping = self.allele_encoding(current_dbsnp_data, current_vcf_data, code_mappings='')
      current_gwas_df = self.gwas_add_code(current_dbsnp_data,current_gwas_df,code_mapping, output='')
    
      gwas_loc=self._folder_path
      gwas_loc=os.path.join(gwas_loc, "gwas_df_folder")
      current_gwas_df.write.parquet(gwas_loc)

      self._gwas_df = spark.read.parquet(gwas_loc)
  

    


  def _loadExtraData(self):
    """
    Load the AxlDFs (specified in the _dataDesc) needed in the create function. 
    """
    spark, _ = get_spark_session_and_context()

    app_loc=self._folder_path
    # GWAS data
    gwas_loc=os.path.join(app_loc, "gwas_df_folder")
    self._gwas_df = spark.read.parquet(gwas_loc)
    # dbSNP VCF data
    dbsnp_loc=os.path.join(app_loc, "dbsnp_df_folder")
    self._dbsnp_df = spark.read.parquet(dbsnp_loc)
    # gVCF data
    vcf_loc=os.path.join(app_loc, "vcf_df_folder")
    self._vcf_df = spark.read.parquet(vcf_loc)

    return



  def update_chromosome(self,vcf_df): 
    """
    The "chromosome" column will be updated to integers 1-25, the old names are in "old_chromosome" column. 
    Ex: NC_000007.13 -> 7 or NC_000008.10 -> 8, X -> 23, Y -> 24, M -> 25

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
    Entries not matching the "rs<digits>" format were assumed to be numerical and remain unchanged.
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
    Encode alleles. Reference alleles become 0s, while altternative alleles are numbered as 1..n
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
    """
    Identifies missing sample files in a given DataFrame based on the 'samples' from metadata_df.
    This method processes the metadata DataFrame to identify any discrepancies in the number of samples across different files.
    
    Args:
    metadata_df (DataFrame): A DataFrame containing file metadata, where each row has a 'key' and 'value' column. This dict must contain a key called "samples", where the names of the samples are in the values as an array.

    Returns:
    tuple: A tuple containing two elements that are lists:
        - The first element is a list of file paths that are missing samples if any discrepancies are found, 
          or ["0"] if all files have the same number of samples.
        - The second element is a list containing the largest set of samples found in the files.
        - The method will return ["-1"] for both elements in case of a mismatch in samples.
          Ex. If file_1.vcf has samples A , B, C , and D. But file_2.vcf has samples C, D, E and F.


    Method does the following:
    1. Filtering the metadata DataFrame for entries where the key is 'samples'.
    2. Converting the 'value' field to an array of values.
    3. Determining the maximum length of these arrays.
    4. Identifying any rows where the array length is less than the maximum, suggesting missing samples.
    5. Comparing smaller arrays against the largest array to identify distinct missing elements.
        
    """

    
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
      return ["0"] , largest_set
    
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
    """
    Modifies VCF data and handles missing samples by filling them in with 0.

    Args:
        missing_file_path_list (list): A list of file paths that have missing sample data.
        full_sample_names_list (list): A list of all sample names to be included in the final DataFrame.
        metadata_df (DataFrame): A DataFrame containing metadata, used to identify missing samples.
        vcf_df (DataFrame): A DataFrame in the Variant Call Format (VCF), containing genetic variant data.

    Returns:
        DataFrame: A unified VCF DataFrame with columns for all samples, where missing values are filled with "0".

    Note:
        This method assumes the metadata and VCF DataFrames are properly formatted and that the sample names in the 
        full_sample_names_list correspond to those in the VCF DataFrame. It utilizes Spark functions for DataFrame operations.
    """
    
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
    """_update vcf_df to match rsIDs in dbSNP using chrom and position, using assembly-specific dbSNP. The original column 'ID" will be renamed to 'oldID'._

    Args:
        dbsnp (Dataframe): dbSNP 
        in_vcf (Dataframe): input vcf data
        out_vcf_df (str): output vcf in parquet, if empty, return Dataframe
        keep (bool): whether or not to keep records in gVCF that have no matching rsIDs in dbSNP, default to False, no keeping
    """
    
    # update column names in dbSNP to match gVCF
    filtered_dbsnp_df = (dbsnp_df
                         .select(F.col('chromosome'), F.col('position'), F.col('ids'))
                         )
      
    filtered_dbsnp_df = filtered_dbsnp_df.dropDuplicates(['chromosome', 'position','ids'])
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
                        .join(filtered_dbsnp_df, on=['chromosome', 'position'], how='left')
                        )
        new_vcf_df = new_vcf_df.dropna(subset=['ids','chromosome','position'])

    if out_vcf_df != '':
      in_vcf_df.write.mode('overwrite').parquet(out_vcf_df)
    
    return new_vcf_df



  def gvcf_to_vsf(self,gvcf_df, metadata_df, output='', min_quality=0, qfilter=False, code_mapping=''):
    """ 
    Convert gVCF to sparse VSF (Variable Sparse Format)  
    Args:
        gvcf_vcf (Dataframe): dataframe of gvcf data
        metadata_df (Dataframe): metadata dataframe
        output (str, optional): output file path if you want the result to be saved to disk. 
        min_quality (int, optional): Filter out low quality variant below this. Relates to QUAL in vcf file. Defaults to 0.
        qfilter (bool, optional): whether or not apply a filter for variant with a'PASS' status. Associated with FILTER in vcf. Defaults, False.
        code_mapping (str, optional): code mapping file to transform allele coding. Defaults to ''.

    Returns:
        Dataframe or None: Dataframe of gVCF after transformed to VSF

    Example Output:
    +------+---------+----+----+-----------+
    |sample|     rsID|code|dose|sample_name|
    +------+---------+----+----+-----------+
    |     3|  2249382|   1| 2.0|    Sample1|
    |     6| 58616815|   1| 2.0|    Sample2|
    |     7|150848999|   1| 2.0|    Sample2|
    +------+---------+----+----+-----------+

    """
    spark, _ = get_spark_session_and_context()

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
          dbsnp (Dataframe): dbsnp object that is in vcf_set format 
          gwas_df (Dataframe): gwas reference Dataframe
          output (str, optional): Output path to save data as a parquet.  
          overwrite (bool, optional): If you want to overwrite an existing output. Defaults to False.

      Returns:
          Dataframe
      """

      ref = (gwas_df
            .join(dbsnp_df.select('chromosome', 'position', F.col('ids').alias('newids')), on=['chromosome', 'position'], how='left')
      )
      ref = ref.dropna(subset=['newids','chromosome','position'])
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
    """
    Enhances a GWAS DataFrame by adding genetic variant codes from a dbSNP DataFrame and mapping these codes.


    Args:
        dbsnp_df (DataFrame): A DataFrame containing dbSNP data. Must contain the following columns: 'ids', 'allele', and 'code'.
        gwas_df (DataFrame): A DataFrame containing GWAS data with relevant columns for joining with dbsnp_df.
        code_mapping (DataFrame): A DataFrame for mapping dbSNP codes to 1000 Genomes Project codes.
        output (str, optional): The file path where the resultant DataFrame should be written in Parquet format. 
                                If empty, the DataFrame is returned instead of being written to a file.

    Returns:
        DataFrame or None: The modified GWAS DataFrame with additional genetic variant codes. Returns None if the DataFrame is written to a file.
        
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
    Method for calculating Polygeneic Risk Scores (PRS). Options min_quality and qfilter need to correspond to the values in gvcf_to_vsf.
    Args:
        gvsf (DataFrame): A DataFrame in the Genomic Variant Sparse Format containing genetic variant data.
        gwas_df (DataFrame): A DataFrame containing Genome-Wide Association Study data.
        min_quality (int or str): The minimum quality parameter, also used for helping name the results folder.
        qfilter (int or str): The qfilter parameter, also used for helping name the results folder.
        output (str, optional): The file path where the resultant data should be written in CSV format. 
                                If empty, the data is not written to a CSV file.

    Returns:
        DataFrame: A Spark DataFrame containing the calculated PRS.

    Note:
        The method assumes the input DataFrames are in the correct format and contain necessary columns for computation.
        It also requires a Spark session to read from and write to parquet files.
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
    Runs whole prs pipeline.
    
    Args:
      input_min_quality (int, optional): Filter out low quality variant below this. Relates to QUAL in vcf file. Defaults to 0.
      input_qfilter (bool, optional): whether or not apply a filter for variant with a'PASS' status. Associated with FILTER in vcf. Defaults, False.

    Returns:
      Dataframe: PRS results

    """

    # fetch current 
    current_vcf_metadata = self._getData("vcf_metadata").df
    current_dbsnp_data = self._dbsnp_df
    current_gwas_df = self._gwas_df
    current_vcf_data = self._vcf_df
    


    gvsf = self.gvcf_to_vsf(current_vcf_data,current_vcf_metadata, output='', min_quality=input_min_quality, qfilter=input_qfilter, code_mapping='')


    results=self.calc_PRS(gvsf=gvsf,gwas_df=current_gwas_df,min_quality=input_min_quality, qfilter=input_qfilter)

    return results