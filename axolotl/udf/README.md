# README #

Source code for the JavaUDF collections of Axolotl.

#### What is a JavaUDF? How do I use it in my pySpark application?
Please search "Java UDF with pySpark".

#### How do I load Axolotl's bundled JavaUDF collections?
* The specific jar file required by Axolotl's pySpark library
will typically be shipped alongside each release version
(see our [Github's releases page](https://github.com/zhongwang/axolotl/releases/)). When you install Axolotl
via pip, you will need to manually download the correct jar
file from the page.

* Alternatively, you can also opt to build the jar file from scratch (see below).

* After downloading, start your Spark session incorporating the
jar file on your driver machine (e.g., `pyspark --jars axolotl_udf_xx.jar`).

#### Build axolotl_udf jar file from scratch

1. Install sbt (see: [https://www.scala-sbt.org/](https://www.scala-sbt.org/))
2. From the axolotl/udf directory, run: `sbt package`
3. sbt will display something along the lines of `[info] compiling 1 Scala source and 7 Java sources to ...`
4. When finished, the compiled jar file will be exported as `/axolotl/udf/target/scala-<version>/axolotl_udf_<version>.jar`

#### Contribute your own Java/Scala UDFs

General guidelines:
* Store Java and Scala codes to their appropriate folders: `/src/main/java` and `/src/main/scala`
	* inside each language-specific folder, put generic functions and classes files into the `libraries` folder, and UDF classes into the `udfs` folder.
* Packages naming
	* `org.jgi.axolotl.udfs.YourClassName` for UDF classes
	* `org.jgi.axolotl.libs.YourClassName` for generic classes
	* Currently, there is no distinction between Scala and Java-based classes in terms of their namespace.
* More detailed contribution guide will come later. If in doubt, please contact us directly via the [issues page](https://github.com/zhongwang/axolotl/issues).