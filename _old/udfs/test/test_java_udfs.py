# Databricks notebook source
# MAGIC %md
# MAGIC # Test Java UDF functions for SpaRC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Java Minimizer function

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
# java udf kmer function
spark.udf.registerJavaFunction("jkmerudf", "org.jgi.spark.udf.jkmer", ArrayType(LongType()))

# COMMAND ----------

from sparc.krmapper import *
from sparc.samples import *

input_seq = 'dbfs:/mnt/gis20mock/gis20mock/pacbio.seq'
output_prefix = 'dbfs:/mnt/gis20mock/gis20mock/pacbio_test'
samples, total_reads = get_samples(spark, input_seq, max_samples=0, max_reads=1000, first_index=0)     

k = 31
minimizer = False
m = int(k/2) + 1 if minimizer else k
n = 0
min_kmer_count = 2

# COMMAND ----------

# Brian's implementation
# only positive hash values are used (lose ~half of kmers), maxmizer instead of minimizer
for s in samples:
    s['rkmapping'] = reads_to_kmersu(spark, s['reads'], k=k, m=m, n=n).select('id', 'sid', 'kmerlist').cache()
    s['kmers'] = kmer_filter(s['rkmapping'], min_kmer_count=min_kmer_count)
    print('{:,d}'.format(s['kmers'].count()) + " kmers/mmers passed filter.")   

# COMMAND ----------

#save kmers
samples = save_samples(spark, samples, output_prefix, key='reads', verbose=False) 
samples = save_samples(spark, samples, output_prefix, key='kmers', verbose=False)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Java edgeGen Function

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
# java udf kmer function
spark.udf.registerJavaFunction("edgegenudf", "org.jgi.spark.udf.edgeGen", ArrayType(ArrayType(LongType())))

# COMMAND ----------

# merge all samples
if len(samples) == 1:
    kmer_df = samples[0]['kmers']
else:    
    kmer_df = (reduce(DataFrame.unionByName, [s['kmers'] for s in samples])
    .groupby('kmer')
    .agg(F.sum("kmer_count").alias("kmer_count"), F.flatten(F.collect_list('Ids')).alias('Ids'))
    )

# COMMAND ----------

max_kmer_count=200
min_edge_weight=2
"""
edges dataframe: src(Integer). dst(Integer), weight(Integer)
udf version
"""
kmer_df = kmer_df.select(F.slice('Ids', 1, max_kmer_count).alias('Ids'))
kmer_df.createOrReplaceTempView("kmer_df")
query = 'SELECT edgegenudf(IDs, {}) as edgelist from kmer_df'.format(max_kmer_count)
kmer_df = spark.sql(query)
edge_df = (kmer_df
.select(F.explode("edgelist").alias('edge'))
.groupby('edge')
.agg(F.count(F.lit(1)).alias("weight"))
.where(F.col("weight") >=min_edge_weight)
.select(F.col('edge')[0].alias('src'), F.col('edge')[1].alias('dst'), 'weight')
)

# COMMAND ----------

edge_df.show(2)

# COMMAND ----------


