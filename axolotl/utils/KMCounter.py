"""
Distributed kmer/minimizer counter utility


For fastq (fastq.gz) inputs, converted to unaligned SAM format first.
e.g. using [Picard](https://gatk.broadinstitute.org/hc/en-us/articles/360036351132-FastqToSam-Picard-):

```
java -jar picard.jar FastqToSam \
       F1=forward_reads.fastq.gz \
       F2=reverse_reads.fastq.gz \
       O=unaligned_read_pairs.sam \
       SM=sample001 \
       RG=rg0013
```

Then read the sam file into a dataframe and performs kmer counting in Axolotl:
```
from axolotl.utils.spark import get_spark_session_and_context
import axolotl.utils.KMCounter.count_kmers
spark, _ = get_spark_session_and_context()

# read the sam file(s)
reads_df = spark.read.csv(sam_file, sep='\t', header=False, comments="@")
# only use the qname and seq columns, rename to 'id' and 'seq'
reads_df = reads_df.select(*(reads_df.columns[0,9])).toDF(['id', 'seq'])

# count kmers k=31 (set m=k)
kmer_count = count_kmers(reads_df, k=31, m=31, min_kmer_count=2)

# count minimizers m=15, w=15
minimizer_count = count_kmers(reads_df, k=31, m=16, min_kmer_count=2)

```

"""
from axolotl.utils.spark import get_spark_session_and_context
spark, _ = get_spark_session_and_context()
spark.udf.registerJavaFunction("jkmerudf", "org.jgi.axolotl.udfs.jkmer", ArrayType(LongType()))

def count_kmers(reads_df, k=31, m=15, min_kmer_count=2):
    """
    given a reads dataframe with columns ['id', 'seq' ...]
    count kmers using udf function
    filter out erroreous kmers by setting min_kmer_count (default 2)

    returns a dataframe with two columns: 
        kmer (long) -> kmer/minimizer hashes
        kmer_count (long) -> counts
    """
    # kmer dataframe: kmer(Integer), kmer_count(Integer). Ids(Array(Integer))

    kmer_df = (reads_to_kmer(reads_df, k=k, m=m, n=0)
            .groupby('kmer')
            .agg(F.count(F.lit(1)).alias("kmer_count"))
            .where(F.col("kmer_count") >=min_kmer_count)
    )
    return kmer_df

def reads_to_kmer(reads_df, k=31, m=15, n=0):
    """
    calculate kmer to ids mapping
    use java UDF
    if k=m, performs kmer 
    otherwise, performs minimizer with w = k-m
    resulting kmer_df has two columns: "id", "kmer" 
    """
    w = k - m
    if w <= 0:
        w = 1 # kmers
    reads_df.createOrReplaceTempView("reads")
    query = 'SELECT id, jkmerudf(seq, {}, {}) as kmerlist from reads'.format(m,w)
    return spark.sql(query).select('id', F.explode("kmerlist").alias('kmer'))

