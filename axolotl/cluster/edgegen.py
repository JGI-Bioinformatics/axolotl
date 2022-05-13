"""

# produce edge triplets
# by zhong wang @lbl.gov

# testing 

from sparc.samples import *
from sparc.edgegen import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# load an existing sample
datapath = 's3://share.jgi-ga.org/gis20mock/pacbio_'
sample1 = Sample(datapath=datapth)  

min_edge_weight=2 # minimum number of shared kmers/minimizers between a pairs of reads to draw an edge
numPartitions = 10

edge_df= (edge_gen(datapath,
                     [sample1],
                     min_edge_weight=min_edge_weight, # minimum number of shared kmers/minimizers between a pairs of reads to draw an edge
                     DEBUG = True,
                     numPartitions=numPartitions
                    )
            )

"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce

from sparc.utils import *
from sparc.evaluation import * 


def edge_gen(output_prefix,
                     samples, # samples is an array of samples
                     max_kmer_count=200,
                     min_edge_weight=2, # minimum number of shared kmers/minimizers between a pairs of reads to draw an edge
                     DEBUG = False,
                     numPartitions=200
                    ):
  
    print("Local clustering. ")
    saving_prefix = output_prefix

    print("Minimum shared kmer/minimizer between reads: %d" % min_edge_weight)
    
    print("Generating edge triplets ... ")
    # merge all samples
    if len(samples) == 1:
        kmer_df = samples[0].kmers
        reads_df = samples[0].reads.select('id')
    else:    
        kmer_df = (reduce(DataFrame.unionByName, [s.kmers for s in samples])
        .groupby('kmer')
        .agg(F.sum("kmer_count").alias("kmer_count"), F.flatten(F.collect_list('Ids')).alias('Ids'))
        )
        reads_df = (reduce(DataFrame.unionByName, [s.reads for s in samples])
        .select('id')
        )
    if DEBUG:
        print('{:,d}'.format(kmer_df.count()) + " total kmers.")

 #   kmer_df = kmer_df.repartition(10 * numPartitions) # temporarily increase the number of partitions for pairs explode
    kmer_df = kmer_df.select('kmer', F.slice('Ids', 1, max_kmer_count).alias('Ids'))
    # pyspark version, at least 5x faster than the scala udf version
    kmer_df = kmer_df.select('kmer', F.explode("Ids").alias('read'))
    edge_df = (kmer_df
        .select(F.col('kmer'), F.col('read').alias('src'))
        .join(kmer_df.select(F.col('kmer'), F.col('read').alias('dst')), on='kmer')
        .where(F.col('src') < F.col('dst'))
        .groupby('src', 'dst')
        .agg(F.count(F.lit(1)).alias("weight"))
        .where(F.col("weight") >=min_edge_weight)
        .repartition(numPartitions, 'src') # decrease the number of partitions to the original
    )


    if DEBUG:
        print('{:,d}'.format(edge_df.count()) + " edges passed filter.")

     # save the local clustering results
    if saving_prefix:
        print("Saving vertice, edge triplets ...")
        reads_df = reads_df.repartition(numPartitions, 'id')
        save_parquet(reads_df, saving_prefix, output_suffix='_vertice', overwrite=True)
        save_parquet(edge_df, saving_prefix, output_suffix='_edges', overwrite=True)
        print("Vertice and Edges saved.")

    return edge_df
        
    

