#!/bin/bash
# Runs the Spark reads and the C10a SQL on 4.3.0 and then 4.0.2.
HERE=$(cd "$(dirname "$0")" && pwd)
bash "$HERE/spark.sh" 4.3
bash "$HERE/spark.sh" 4.0
