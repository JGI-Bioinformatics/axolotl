package org.jgi.axolotl.udfs;

/*  jkmer was from Brain Bushnell's quick implementation

# Example usage:
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType
spark = SparkSession.builder.getOrCreate()
spark.udf.registerJavaFunction("jkmerudf", "org.jgi.spark.udf.jkmer", ArrayType(LongType()))

from pyspark.sql.functions import regexp_replace 
# input_seq is a fastq file in parquet format with four fields (id, name, seq, qual) 
input_seq = spark.read.parquet('dbfs:/mnt/share.jgi-ga.org/contamination/zymo/short-reads/czwon_full3/czwon.seq')

# access the dataframe via SQL interface, create a table named "reads" for us to do SQL
input_seq.createOrReplaceTempView('reads')
# this is the SQL select syntax
sql_cmd = """SELECT id, jkmerudf(seq) as kmers from reads"""
input_seq = spark.sql(sql_cmd)
# input_seq is now a dataframe with readid -> kmer array

*/

import org.jgi.axolotl.libs.*;
import org.apache.spark.sql.api.java.UDF3;

public class jkmer implements UDF3<String, Integer, Integer, long[]> {
    @Override
    public long[] call(String seq, Integer k, Integer w) throws Exception {
        Minimizer minnow = new Minimizer(k, w);
		long[] array = minnow.minimize(seq.getBytes());
        return array;
    }

}