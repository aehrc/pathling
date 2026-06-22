#!/bin/bash
# Copyright © 2025, Commonwealth Scientific and Industrial Research Organisation (CSIRO)
# ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.
#
# @author John Grimes
#
# Role-dispatching entrypoint for the dual-role Pathling server image. The image
# runs as either the FHIR server (the default) or a Spark Kubernetes executor,
# selected by the container's first argument. Spark on Kubernetes launches
# executor pods from this image with the single argument "executor" and no
# command override, so the role selection must live in the image entrypoint.
#
# Modified from the Spark Kubernetes Docker image entrypoint script:
# https://github.com/apache/spark/blob/v4.0.2/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh

set -e

# The Pathling JVM module options required on Java 21. Both roles must run with
# these, so they are defined once here and referenced from each branch. They
# were previously supplied by the Jib build's jvmFlags, which Jib ignores once
# an explicit entrypoint is configured.
PATHLING_JVM_OPTS=(
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
)

case "$1" in
  # The "executor" argument runs the container as a Spark Kubernetes executor.
  executor)
    echo "Starting Pathling in Spark executor role." >&2
    # Collect the per-executor JVM options Spark injects via SPARK_JAVA_OPT_*.
    # The grep returns a non-zero status when there are no matches, so it is
    # guarded to avoid aborting the script under "set -e".
    readarray -t SPARK_EXECUTOR_JAVA_OPTS < <(env | grep SPARK_JAVA_OPT_ | sort -t_ -k4 -n | sed 's/[^=]*=\(.*\)/\1/g' || true)
    CMD=(
      java
      "${PATHLING_JVM_OPTS[@]}"
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
      -Xms"$SPARK_EXECUTOR_MEMORY"
      -Xmx"$SPARK_EXECUTOR_MEMORY"
      -cp @/app/jib-classpath-file
      org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend
      --driver-url "$SPARK_DRIVER_URL"
      --executor-id "$SPARK_EXECUTOR_ID"
      --cores "$SPARK_EXECUTOR_CORES"
      --app-id "$SPARK_APPLICATION_ID"
      --hostname "$SPARK_EXECUTOR_POD_IP"
      --resourceProfileId "$SPARK_RESOURCE_PROFILE_ID"
      --podName "$SPARK_EXECUTOR_POD_NAME"
    )
    ;;

  # Any other argument (including none) runs the FHIR server.
  *)
    echo "Starting Pathling in FHIR server role." >&2
    CMD=(
      java
      "${PATHLING_JVM_OPTS[@]}"
      -cp @/app/jib-classpath-file
      au.csiro.pathling.PathlingServer
    )
    ;;
esac

# In dry-run mode, print the resolved command for testing instead of running it.
if [ -n "${PATHLING_ENTRYPOINT_DRY_RUN:-}" ]; then
  echo "${CMD[@]:-}"
else
  exec "${CMD[@]}"
fi
