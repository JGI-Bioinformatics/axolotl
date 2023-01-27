.. Lumache documentation master file, created by
  sphinx-quickstart on Sun Jun 26 22:25:02 2022.
  You can adapt this file completely to your liking, but it should at least
  contain the root `toctree` directive.
 
Welcome to Axolotl's documentation!
===================================
Purpose
-------
 
An easy-to-use, pySpark-based, scalable library for big data genomics. 



  
 
 
Background
----------
 
Metagenomic datasets and their intermediate results from a typical project are measured to the order of Terabytes up to the Petabyte scale.  For example, the Mushroom Hot Springs Metagenome project has generated 2 terabases of sequence and requires 200-500 terabytes of disk space for analysis. JGI has been levaging DataBricks Spark via AWS EMR to perform large-scale metagenome analysis.  Preliminary results suggest that this architecture, combining automatic data parallelism with reproducible notebook-based analysis, could be an ideal solution for genomics data scientists to scale up their data pipelines.
 
The goal of this project is to port our existing single-machine based software algorithms (in Java, C++, or Python) on the Spark platform and add them to a scalable metagenomics library.
 
 
Goal
----
The goal of this project is to port our existing single-machine based software algorithms (in Java, C++, or Python) on the Spark platform and add them to a scalable metagenomics library.

.. toctree::
  :maxdepth: 3
  :caption: Contents:
 
 
 

 

 
.. toctree::
   :caption: This Section
   :hidden:
  
  
   DataBricks
   QC-analysis
   Spark
   Installation
   FAQ
  
  
  


.. toctree::
 
  