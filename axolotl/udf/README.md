# README #

User defined functions (Java) for Axolotl

### How do I add new functions? ###

* Develop the Java code (e.g., Minimizer.java)
* Add a UDF interface (e.g., jkmer.java)
* Install sbt if not installed, then run "sbt package"
* The jar file will be located at udf/target/scala_xx_xx/axolotl_udf_xx_xx.jar 
* (DataBricks) Start a Spark cluster, install the library (jar file)
* Test your new function as directed

### Contribution guidelines ###

* Make a brunch to add new function
* Only to main after fully tested
* Other guidelines

### Who do I talk to? ###

* Zhong Wang @ lbl.gov
* Sean Saki @lbl.gov