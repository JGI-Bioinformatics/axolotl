Registering a new Java function
===============================

Input
-----
``spark.udf.registerJavaFunction`` takes three arguments.
    * Function name to be used in spark sql
    * Java class name that implements UDF
    * The return type of UDF

``spark.sql"""SELECT (name of the file) ("file") """).show()``
    * SELECT is used to select data from a database.

.. Code-block::

    from pyspark.sql.types import *
    spark.udf.registerJavaFunction("getGC", "org.jgi.spark.udf.gc", DoubleType())
    spark.sql("""SELECT getGC("AGCGGGTGGAGGGTGGGANGGTGGTGTGGGTAAGGTGT")""").show()

Output
------
.. code-block::

    +---------------------------------------------+
    |getGC(AGCGGGTGGAGGGTGGGANGGTGGTGTGGGTAAGGTGT)|
    +---------------------------------------------+
    |                            64.86486486486487|
    +---------------------------------------------+

Explanation
------------
.. code-block::

    from pyspark.sql.types import *
    spark.udf.registerJavaFunction("getGC", "org.jgi.spark.udf.gc", DoubleType())
    spark.sql("""SELECT getGC("AGCGGGTGGAGGGTGGGANGGTGGTGTGGGTAAGGTGT")""").show()


User Defined Functions are used in Spark SQL for custom transformations of data


    *  The data type representing floating point numbers
    * ``spark.sql("""SELECT getGC("AGCGGGTGGAGGGTGGGANGGTGGTGTGGGTAAGGTGT")""").show()`` is used to test the function
    * ``.show`` is  used to show the output of the function
    

    