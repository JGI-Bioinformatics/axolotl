#!/bin/bash
# deploy build to databricks
# make sure nobody else is using the cluster!!
# `./deploy_databricks.bash $clusterID`
# eg. `./deploy_databricks.bash zw_single`

#!/usr/bin/env bash
# spawn a small cluster to run spark applications
# by Zhong Wang @ lbl.gov

# defaults
cluster_name="zw_single"

# usage message
usage ()
{
echo "<Deploy axolotl to a Databricks cluster. --help>
        --cluster           databricks cluster name, required
Example: $0 --cluster zw_single "
exit 0
}
if [ $# -lt 2 ]; then
	usage
fi

# get input parameters
while [ $# -gt 0 ]; do
    case "$1" in
    --cluster)
      shift
      cluster_name=$1
      ;;
    --help)
      shift
      usage
      ;;
    -*)
      echo "Unknown options: $1"
      usage
      ;;
    *)
      break;
      ;;
    esac
    shift
done

VERSION=`grep "version=" setup.py |awk -F\' '{print $2,$4}'| xargs echo -n`
echo "building version: $VERSION"
if [[ $VERSION == "" ]]
then
    echo "Could not determine library version"
    exit 0
fi
ClusterID=`databricks clusters list |grep $cluster_name |cut -f1 -d ' '| xargs echo -n`
echo "Deploying to clusterID: $ClusterID"
if [[ $ClusterID == "" ]]
then
    echo "Could not determine cluster id from the name \"${cluster_name}\". Make sure the cluster exists."
    exit 0
fi

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
# uninstall the old one first
databricks libraries uninstall --cluster-id $ClusterID --whl $deploydir/$deploy
CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`
if [[ $CLUSTERSTATE == "TERMINATED" ]] 
then
    databricks clusters start --cluster-id $ClusterID
else
    databricks clusters restart --cluster-id $ClusterID
fi
# wait until the cluster is up and running, then install
CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`
while [ $CLUSTERSTATE != 'RUNNING' ]
do
  sleep 30
  CLUSTERSTATE=`databricks clusters get --cluster-id $ClusterID |grep \"state\":|cut -f2 -d ":"|awk -F\" '{print $2,$4}'| xargs echo -n`

done
databricks libraries install --cluster-id $ClusterID --whl $deploydir/$deploy