#!/bin/bash
# Copyright © 2025, Commonwealth Scientific and Industrial Research Organisation (CSIRO)
# ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.
#
# @author John Grimes
#
# Dry-run test of the dual-role image entrypoint's role-selection logic. It runs
# the entrypoint with PATHLING_ENTRYPOINT_DRY_RUN set, capturing the resolved
# start-up command rather than executing it, and asserts on that command. The
# test needs no Docker, no network, and no cluster.

set -euo pipefail

# Resolve the entrypoint path relative to this script so the test works
# regardless of the working directory it is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENTRYPOINT="$SCRIPT_DIR/../../main/jib/usr/bin/entrypoint.sh"

# Dry-run mode makes the entrypoint print the resolved command instead of
# exec-ing it.
export PATHLING_ENTRYPOINT_DRY_RUN=1

PASS=0
FAIL=0

# Asserts that the resolved command contains an expected substring.
assert_contains() {
  local label="$1" haystack="$2" needle="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    echo "  PASS: $label"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $label"
    echo "    expected to contain: $needle"
    echo "    actual: $haystack"
    FAIL=$((FAIL + 1))
  fi
}

# Asserts that the resolved command does not contain a substring.
assert_not_contains() {
  local label="$1" haystack="$2" needle="$3"
  if [[ "$haystack" != *"$needle"* ]]; then
    echo "  PASS: $label"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $label"
    echo "    expected NOT to contain: $needle"
    echo "    actual: $haystack"
    FAIL=$((FAIL + 1))
  fi
}

# User Story 1: the "executor" argument resolves to a Spark executor backend.
echo "User Story 1: executor role"
executor_cmd="$(env \
  SPARK_EXECUTOR_MEMORY=1g \
  SPARK_DRIVER_URL=spark://driver:7077 \
  SPARK_EXECUTOR_ID=1 \
  SPARK_EXECUTOR_CORES=2 \
  SPARK_APPLICATION_ID=app-test \
  SPARK_EXECUTOR_POD_IP=10.0.0.1 \
  SPARK_RESOURCE_PROFILE_ID=0 \
  SPARK_EXECUTOR_POD_NAME=exec-1 \
  SPARK_JAVA_OPT_0=-Dsample.java.opt=on \
  bash "$ENTRYPOINT" executor 2>/dev/null)"

assert_contains "launches the Spark Kubernetes executor backend" "$executor_cmd" \
  "org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend"
assert_contains "passes the driver URL" "$executor_cmd" "--driver-url spark://driver:7077"
assert_contains "passes the executor id" "$executor_cmd" "--executor-id 1"
assert_contains "passes the executor cores" "$executor_cmd" "--cores 2"
assert_contains "passes the application id" "$executor_cmd" "--app-id app-test"
assert_contains "passes the hostname" "$executor_cmd" "--hostname 10.0.0.1"
assert_contains "passes the resource profile id" "$executor_cmd" "--resourceProfileId 0"
assert_contains "passes the pod name" "$executor_cmd" "--podName exec-1"
assert_contains "sizes the minimum heap from executor memory" "$executor_cmd" "-Xms1g"
assert_contains "sizes the maximum heap from executor memory" "$executor_cmd" "-Xmx1g"
assert_contains "applies the Spark-injected JVM options" "$executor_cmd" "-Dsample.java.opt=on"
assert_contains "uses the generated classpath file" "$executor_cmd" "@/app/jib-classpath-file"
assert_not_contains "does not launch the FHIR server" "$executor_cmd" \
  "au.csiro.pathling.PathlingServer"

# User Story 2: no argument (and an unrecognised argument) resolve to the FHIR
# server.
echo "User Story 2: default server role"
server_cmd="$(bash "$ENTRYPOINT" 2>/dev/null)"
unknown_cmd="$(bash "$ENTRYPOINT" anything-else 2>/dev/null)"

assert_contains "no argument launches the FHIR server" "$server_cmd" \
  "au.csiro.pathling.PathlingServer"
assert_contains "no argument uses the generated classpath file" "$server_cmd" \
  "@/app/jib-classpath-file"
assert_not_contains "no argument does not launch the executor backend" "$server_cmd" \
  "KubernetesExecutorBackend"
assert_contains "unrecognised argument launches the FHIR server" "$unknown_cmd" \
  "au.csiro.pathling.PathlingServer"
assert_not_contains "unrecognised argument does not launch the executor backend" \
  "$unknown_cmd" "KubernetesExecutorBackend"

# User Story 3: both roles carry the three Pathling JVM module options.
echo "User Story 3: JVM module options in both roles"
for opt in \
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
  "--add-opens=java.base/java.net=ALL-UNNAMED" \
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"; do
  assert_contains "executor command carries $opt" "$executor_cmd" "$opt"
  assert_contains "server command carries $opt" "$server_cmd" "$opt"
done

# Pass/fail summary. Exits non-zero if any assertion failed.
echo
echo "Passed: $PASS, Failed: $FAIL"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
