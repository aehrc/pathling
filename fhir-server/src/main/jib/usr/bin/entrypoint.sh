#!/bin/bash
#
# Copyright 2023 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modified from the Kubernetes Docker image entrypoint script: 
# https://github.com/apache/spark/blob/v3.4.0/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh

set -xe

case "$1" in
  # If the command is "executor", run the container as a Kubernetes executor.
  executor)
    env | grep SPARK_JAVA_OPT_ | sort -t_ -k4 -n | sed 's/[^=]*=\(.*\)/\1/g' > /tmp/java_opts.txt
    readarray -t SPARK_EXECUTOR_JAVA_OPTS < /tmp/java_opts.txt
    CMD=(
      java
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
      -Xms$SPARK_EXECUTOR_MEMORY
      -Xmx$SPARK_EXECUTOR_MEMORY
      -cp @/app/jib-classpath-file
      org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend
      --driver-url $SPARK_DRIVER_URL
      --executor-id $SPARK_EXECUTOR_ID
      --cores $SPARK_EXECUTOR_CORES
      --app-id $SPARK_APPLICATION_ID
      --hostname $SPARK_EXECUTOR_POD_IP
      --resourceProfileId $SPARK_RESOURCE_PROFILE_ID
      --podName $SPARK_EXECUTOR_POD_NAME
    )
    ;;

  # If the command is anything else, run the FHIR server.
  *)
    CMD=(
      java
      -cp @/app/jib-classpath-file
      au.csiro.pathling.PathlingServer
    )
    ;;
esac

exec "${CMD[@]}"
