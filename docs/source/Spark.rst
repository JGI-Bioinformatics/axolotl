Spark
========

Apache Spark is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

Installation:
-------------
To Install Spark, visit `Apache Spark <https://spark.apache.org/>`_ to choose which package type and which spark release and download it.

**PySpark** is the Python API for Apache Spark, an open source, distributed computing framework and set of libraries for real-time, large-scale data processing.  `Apache Spark <https://spark.apache.org/>`_ offers a *simple* and
*intuitive* API.

To use PySpark, first install it using pip:
 
.. code-block:: console
 
   $ !pip install pyspark
 
=====


Spark functions used in Axolotl
---------------------------------
  * ``.avg`` is used to return the average value from a particular column in the DataFrame.
  * ``getItem()`` extracts a value from the column
  * ``monotonically_increasing_id()`` is used to assign row number and is guaranteed to be monotonically increasing and unique
  * ``.withColumn`` is used to add a new a column 
  * ``.select`` is used to select multiple columns
  * ``.show`` is  used to show the output of the function
  * ``spark.read.text()`` is used to read a file or directory of text files into a Spark DataFrame
  * ``.lit`` is used to add a new column to DataFrame by assigning a literal or constant value
  
  

