def get_spark_session_and_context():
    spark = SparkSession.getActiveSession()
    if spark == None:
        raise Exception("can't find any Spark active session!")
    sc = spark.sparkContext

    return (spark, sc)