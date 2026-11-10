"""Driver for the compute-first run on Databricks, a Python task. The rule is never installed.
Usage: run_pbg.py <base dir> <out dir>.
"""
import json
import sys
import traceback

from pyspark.sql import SparkSession

base, out = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.getOrCreate()
jvm, jspark = spark._jvm, spark._jsparkSession
jvm.spike.LambdaPruning.uninstall(jspark)
import os
rt = {"sparkVersion": spark.version, "DATABRICKS_RUNTIME_VERSION": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
      "scala": jvm.scala.util.Properties.versionString(), "java": jvm.java.lang.System.getProperty("java.version"),
      "extraOptimizationsSize": jspark.experimental().extraOptimizations().size()}
print("RUNTIME", json.dumps(rt), flush=True)
jvm.spike.DbxProbe.writeText(jspark, f"{out}/runtime.json", json.dumps(rt))
print("extraOptimizations size:", jspark.experimental().extraOptimizations().size(), flush=True)
for variant in ["parquet", "delta-dv"]:
    try:
        r = jvm.spike.PbgViews.run(jspark, "dbfs:/lpr/ndjson", "dbfs:/lpr/small", base, out, variant)
        print("PBG-DONE", variant, len(r), flush=True)
    except Exception:
        print("PBG-FAILED", variant, traceback.format_exc()[-3000:], flush=True)
        jvm.spike.DbxProbe.writeText(jspark, f"{out}/pbg-{variant}-error.json",
                                     json.dumps({"error": traceback.format_exc()}))
