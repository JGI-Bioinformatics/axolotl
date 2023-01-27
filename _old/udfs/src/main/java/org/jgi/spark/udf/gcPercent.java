package org.jgi.spark.udf;
import org.apache.spark.sql.api.java.UDF1;

/*  calGC was from Brain Bushnell's quick implementation

# Example usage:
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
spark.udf.registerJavaFunction("gcPercent", "org.jgi.spark.udf.gcPercent", DoubleType())
spark = SparkSession.builder.getOrCreate()

# input_seq is a fastq file in parquet format with four fields (id, name, seq, qual) 
input_seq = spark.read.parquet('dbfs:/mnt/share.jgi-ga.org/contamination/zymo/short-reads/czwon_full3/czwon.seq')

# access the dataframe via SQL interface, create a table named "reads" for us to do SQL
input_seq.createOrReplaceTempView('reads')
# this is the SQL select syntax
sql_cmd = """SELECT id, name, qual, seq, gcPercent(seq) as GC_percent from reads"""
input_seq = spark.sql(sql_cmd)
input_seq.show(2, truncate=False, vertical=True)

*/
public class gcPercent implements UDF1<String, Double> {
    @Override
    public Double call(String read) throws Exception {
        return calcGC(read);
    }
    private double calcGC(String read) {
        byte[] seq = read.getBytes();
        
        int[] counts = new int[128];
        for(byte b : seq){counts[b]++;}
        int A = counts['a']+counts['A'];
        int C = counts['c']+counts['C'];
        int G = counts['g']+counts['G'];
        int T = counts['t']+counts['T'];
        double gc = (G+C)*100.0/(A+C+G+T);
        return gc;
       }
    
   
}