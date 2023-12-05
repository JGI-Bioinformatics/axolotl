from pyspark.sql import SparkSession


def get_spark_session_and_context():
    spark = SparkSession.getActiveSession()
    if spark == None:
        raise Exception("can't find any Spark active session!")
    sc = spark.sparkContext

    return (spark, sc)


def activate_udf(udf_name, output_type=None, package_path="org.jgi.axolotl.udfs"):
    spark, sc = get_spark_session_and_context()
    spark.udf.registerJavaFunction(udf_name, "{}.{}".format(package_path, udf_name), output_type)