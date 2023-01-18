# README #

User defined functions (Java) for SpaRC

### How do I add new functions? ###

* Develop the Java code (e.g., Minimizer.java)
* Add a UDF interface (e.g., jkmer.java)
* (VScode)Export the Jar, only the target/classes, no main classes
* Deposite the jar sparcudf.jar to dbfs:/FileStore/jars/sparc/
* Start a Spark cluster, install the library
* Test your new function as directed

### Contribution guidelines ###

* Make a brunch to add new function
* Only to main after fully tested
* Other guidelines

### Who do I talk to? ###

* Zhong Wang @ lbl.gov
* Sean Saki @lbl.gov