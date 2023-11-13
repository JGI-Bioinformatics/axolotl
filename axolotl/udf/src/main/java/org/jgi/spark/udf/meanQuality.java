package org.jgi.spark.udf;
/*
 
# Example usage:
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
spark.udf.registerJavaFunction("meanQuality", "org.jgi.spark.udf.meanQuality", DoubleType())
spark = SparkSession.builder.getOrCreate()

# input_seq is a fastq file in parquet format with four fields (id, name, seq, qual) 
input_seq = spark.read.parquet('dbfs:/mnt/share.jgi-ga.org/contamination/zymo/short-reads/czwon_full3/czwon.seq')

# access the dataframe via SQL interface, create a table named "reads" for us to do SQL
input_seq.createOrReplaceTempView('reads')
# this is the SQL select syntax
sql_cmd = """SELECT id, name, qual, seq, meanQuality(seq) as mean_quality from reads"""
input_seq = spark.sql(sql_cmd)
input_seq.show(2, truncate=False, vertical=True)
 */


import org.apache.spark.sql.api.java.UDF1;

public class meanQuality implements UDF1<String, Double> {
    @Override
    public Double call(String q) throws Exception {
        return meanQ(q);
    }
    
    private double meanQ(String q) {
    	double total = 0;
    	for(int i = 0; i < q.length(); i++) {
    		total += q.charAt(i);
    	}
    	return (total/((double) q.length()))-33;
    }
}

/*
 * This function is only meant to find the average of the quality scores based on a quality string input. This is not standard practice as it is 
 * generally useful to convert from Q score to probability back to Q score so that the input/output are consistent and so that the parameters
 * are more well understood by the user for example a Q score of 20 would be a error of 1% but instead of having the user input either 1% error
 * or 99% correct we can leave them to input 20 and have that as the Q score even though our internal calculations will always use percentages.
 */