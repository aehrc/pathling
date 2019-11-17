#!/bin/bash
set -x -e

CLISIGHT_RELEASE_S3=s3://csiro-clinsight/deploy/release/1.0.0-latest
IS_MASTER=false

if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

while [ $# -gt 0 ]; do
    case "$1" in
    --release)
      shift
      CLISIGHT_RELEASE_S3=$1
      ;;
    -*)
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
    esac
    shift
done

upstart_clinsight() {
  #setup upstart execution
  sudo tee /etc/init/clinsight.conf > /dev/null << EOF
# Clinsight

description "Clinsight"
author      "szu004"

respawn
respawn limit 0 10

console output

chdir /home/hadoop

script
  
su - hadoop > /home/hadoop/clinsight.log 2>&1 <<BASH_SCRIPT
export CLINSIGHT_SPARK_MASTER_URL=yarn-client
export CLINSIGHT_HTTP_PORT=8888
export CLINSIGHT_TERMINOLOGY_SERVER_URL=https://r4.ontoserver.csiro.au/fhir
export CLINSIGHT_EXECUTOR_MEMORY=4G
export CLINSIGHT_WAREHOUSE_URL=hdfs:///user/spark/warehouse
spark-submit --class au.csiro.clinsight.FhirServerContainer --conf spark.executor.userClassPathFirst=true --conf spark.driver.userClassPathFirst=true fhir-server-shaded.jar 
BASH_SCRIPT
      
end script
EOF
}


if [ "$IS_MASTER" = true ]; then
  aws s3 cp ${CLISIGHT_RELEASE_S3}/fhir-server-shaded.jar ${HOME}
  #Setup daemons
  upstart_clinsight
fi