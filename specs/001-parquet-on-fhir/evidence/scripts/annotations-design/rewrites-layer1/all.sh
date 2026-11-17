#!/bin/bash
# Builds and runs everything for one Spark version. Usage: all.sh 4.3|4.0
set -e
cd "$(dirname "$0")"
bash build.sh "$1"
bash run.sh "$1" > /dev/null
bash semantic.sh "$1" > /dev/null
echo "done $1"
