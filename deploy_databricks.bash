#!/bin/bash
# deploy build to databricks
# make sure nobody else is using the cluster!!
# `./deploy_databricks.bash $clusterID`
# eg. `./deploy_databricks.bash zw_single`

VERSION=`grep "version=" setup.py |awk -F\' '{print $2,$4}'| xargs echo -n`
echo "building version: $VERSION"
cluster_name=$1
ClusterID=`databricks clusters list |grep $cluster_name |cut -f1 -d ' '| xargs echo -n`
echo "Deploying to clusterID: $ClusterID"
# python
deploydir="dbfs:/FileStore/site_python"
deploy="axolotl-${VERSION}-py2.py3-none-any.whl"
 
rm -rf build/ dist/ axolotl.egg-info/
python setup.py sdist bdist_wheel
#pip wheel -r requirements.txt
databricks fs rm $deploydir/$deploy
echo "deploying $deploydir/$deploy"
databricks fs cp dist/$deploy $deploydir/$deploy
#Install it on a cluster
CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`
if [[ $CLUSTERSTATE == "TERMINATED" ]] 
then
    databricks clusters start --cluster-id $ClusterID
fi
CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`
while [ $CLUSTERSTATE != 'RUNNING' ]
do
  sleep 30
  CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`

done
databricks libraries uninstall --cluster-id $ClusterID --whl $deploydir/$deploy
databricks clusters restart --cluster-id $ClusterID
CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`
while [ $CLUSTERSTATE != 'RUNNING' ]
do
  sleep 30
  CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`

done
databricks libraries install --cluster-id $ClusterID --whl $deploydir/$deploy