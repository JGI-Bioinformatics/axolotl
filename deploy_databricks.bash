#!/bin/bash
# deploy build to databricks
# `./deploy_databricks.bash $version $clusterID`
# use `databricks clusters list` to find out the clusterID
# eg. `./deploy_databricks.bash 0.1.0 0719-225420-k5zj97kt`

VERSION=$1
ClusterID=$2
# python
deploydir="dbfs:/FileStore/site_python"
deploy="axolotl-${VERSION}-py2.py3-none-any.whl"
 
rm -rf build/ dist/ axolotl.egg-info/
python setup.py bdist_wheel
#pip wheel -r requirements.txt
databricks fs rm $deploydir/$deploy
echo "deploying $deploydir/$deploy"
databricks fs cp dist/$deploy $deploydir/$deploy
#Install it on a cluster
databricks libraries uninstall --cluster-id $ClusterID --whl $deploydir/$deploy
databricks libraries install --cluster-id $ClusterID --whl $deploydir/$deploy