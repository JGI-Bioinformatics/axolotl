name := "axolotl_udf"
 
version := "1.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.0"
val sparkVersion = "3.4.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
