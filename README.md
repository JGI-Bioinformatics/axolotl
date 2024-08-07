<img src="https://github.com/zhongwang/axolotl/blob/dev/Axolotl_logo1_transparent.png?raw=true" width="300">

### Overview
Axolotl  is a Python library for scalable distributed genome and metagenome data analysis. Existing tools and systems that we rely on are struggling to keep up with the rapid explosion of genomic data. Compounding this issue, developing scalable solutions require a steep learning curve in parallel programming, which presents a barrier to academic researchers. While we do have scalable solutions for specific tasks, we lack comprehensive, end-to-end solutions. It's this gap in our toolkit that we aim to address with Axolotl.

The name "Axolotl" for the Python library is rich in symbolism and significance, each aspect inspired by the unique biological features of the axolotl, a fascinating amphibian:

1. **Large Genome - Symbolizing Big Genomic Data**: 
   The axolotl is known for having one of the largest genomes among animals, approximately ten times the size of the human genome. This characteristic reflects the library's capacity to handle "big genomic data", which is essential in the context of genomic and metagenomic analysis where vast amounts of data are the norm.

2. **Regeneration Ability - Signifying Robust Software**: 
   Axolotls have an extraordinary ability to regenerate complete limbs, organs, and even parts of their brain, a feature that symbolizes the robustness of the software. Just as the axolotl can heal and regenerate, the Axolotl library is designed to be resilient and capable of recovering or adapting to various computational challenges, ensuring continuous and reliable performance in data analysis.

3. **External Gills - Representing Parallel, Scalable Computing**: 
   Axolotls possess multiple external gills that maximize the absorption of oxygen from water. This feature is symbolic of parallel, scalable computing. Just as the gills efficiently process oxygen in parallel, the Axolotl library is built for parallel processing, efficiently handling multiple tasks or large datasets simultaneously, and scaling up to meet the demands of extensive genomic data analysis.

4. **Ease of Care - Signifying User-Friendly Software**: 
   The axolotl is known to be relatively easy to care for as a pet, requiring minimal specialized attention. This aspect of the axolotl symbolizes the user-friendliness of the software library. The Axolotl library is designed to be accessible and manageable, with a reduced learning curve despite its powerful capabilities. This makes it more approachable for academic researchers who might not have extensive experience in parallel programming, allowing them to focus more on their research and less on the complexities of the software. 

In summary, the name "Axolotl" for the Python library is a metaphorical representation of its characteristics - capable of handling large-scale genomic data, robust and resilient in function, efficient in parallel processing, and user-friendly for researchers.



### How to Install Axolotl on HPC
Installation: 
Clean, Create, and Activate Conda Environment: 
Make sure conda-forge is added to your channel before doing this
```
conda clean -a 
conda create -n <NEW_ENV> python=3.11 openjdk=8.0
conda activate  <NEW_ENV> 
```

Download, tar, and install Spark 
Go to https://spark.apache.org/downloads.html
Use wget to download Spark into the Bin of Conda Environment
```
cd $CONDA_PREFIX/bin
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvzf spark-3.5.1-bin-hadoop3.tgz
```

Write a new spark_activate.sh script
```
vi $CONDA_PREFIX/etc/conda/activate.d/spark_activate.sh 

--------------------------------------------------------------------------------
#!/bin/bash

export SPARK_HOME_CONDA_BACKUP=${SPARK_HOME:-}
export SPARK_HOME=$CONDA_PREFIX/bin/spark-3.5.0-bin-hadoop3
--------------------------------------------------------------------------------
```

Write a new spark_deactivate.sh script
```
vi $CONDA_PREFIX/etc/conda/deactivate.d/spark_deactivate.sh 

--------------------------------------------------------------------------------
#!/bin/bash

export SPARK_HOME=$SPARK_HOME_CONDA_BACKUP
unset SPARK_HOME_CONDA_BACKUP
if [ -z $SPARK_HOME ]; then
        unset SPARK_HOME
fi
--------------------------------------------------------------------------------
```

Install PySpark and Axolotl
```
pip install pyspark
pip install git+https://github.com/JGI-Bioinformatics/axolotl.git@main
```

Create start-worker-and-wait.sh custom script (so that we can spawn workers with sbatch instead of having to do a single srun each time)
```
vi $SPARK_HOME/sbin/start-worker-and-wait.sh 
--------------------------------------------------------------------------------

#!/usr/bin/env bash
$SPARK_HOME/sbin/start-worker.sh $@
while true
do
  sleep 10000
done
--------------------------------------------------------------------------------

chmod +x $SPARK_HOME/sbin/start-worker-and-wait.sh
```



### Running Spark on HPC Cluster through Interactive Jupyter Notebook
Get Master Node
On Master Terminal on HPC
```
srun .... --pty /bin/bash

conda activate <NEW_ENV>
cd $CONDA_PREFIX/bin/spark-3.5.0-bin-hadoop3/sbin
./start-master.sh
```

Now cd to where you want to start your jupyter notebook server
Starting Jupyter Notebook server
```
jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser
```

On Worker Terminal on HPC
```
conda activate <NEW_ENV>

#cd to where you saved start-worker-and-wait.sh, which is a custom script you wrote in installation 

sbatch <your_normal_parameters> -n 1 -c <cpu_per_node> -m <mem_per_node> --array=1-<how_many_nodes_you_want> start-worker-and-wait.sh spark://<master_node_ip_and_port> -c <num_cores_you_got> -m <num_memory_you_got>
```


On Local Terminal
```
echo <master_node> | xargs -I {} ssh -N -L localhost:8888:{}:8888 -L localhost:4040:{}:4040 -L localhost:18080:{}:18080 -L localhost:8080:{}:8080 <your_HPC_username>
```

On Web Browser:
For Python Notebook go to localhost:8888,
For History Server go to localhost:18080,
For Spark UI go to localhost:4040,
For Executor go to localhost:8080



### Copyright Notice

Axolotl: a scalable genomics library based on Apache Spark (Axolotl)
Copyright (c) 2024, The Regents of the University of California, through 
Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit others to do so.

### Published papers
* ...
