#!/bin/bash
# Runs every measurement. Usage: all.sh [rows]. Data and logs go to the spike directory.
HERE=$(cd "$(dirname "$0")" && pwd)
bash "$HERE/gen.sh" "${1:-200000}"
bash "$HERE/spark.sh" 4.3
bash "$HERE/spark.sh" 4.0
bash "$HERE/duck.sh"
bash "$HERE/size.sh"
