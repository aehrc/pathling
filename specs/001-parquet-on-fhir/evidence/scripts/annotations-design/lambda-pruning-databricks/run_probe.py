"""Driver for the Databricks run, a Python task.

Installs the rule from Python through py4j, checks a few cases through PySpark, then runs the
Scala matrix. Usage: run_probe.py <smoke|full> <base dir> <out dir> <rows>.
"""
import json
import sys
import traceback

from pyspark.sql import SparkSession

mode, base, out, nrows = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
spark = SparkSession.builder.getOrCreate()
report = {"mode": mode, "sparkVersion": spark.version}


def save(name, obj):
    spark._jvm.spike.DbxProbe.writeText(
        spark._jsparkSession, f"{out}/{name}.json", json.dumps(obj, indent=1, default=str))


# Check a, Python half: py4j must be available, and the rule must install and load.
report["hasPy4j"] = hasattr(spark, "_jsparkSession") and hasattr(spark, "_jvm")
print("py4j available:", report["hasPy4j"], flush=True)
if not report["hasPy4j"]:
    save("python-check", report)
    sys.exit(0)
jvm, jspark = spark._jvm, spark._jsparkSession
try:
    jvm.spike.LambdaPruning.reset()
    jvm.spike.LambdaPruning.install(jspark)
    report["installedViaPy4j"] = jspark.experimental().extraOptimizations().size()
    print("installed via py4j, extraOptimizations size:", report["installedViaPy4j"], flush=True)
except Exception:
    report["installError"] = traceback.format_exc()
    print(report["installError"], flush=True)
    save("python-check", report)
    raise

# The queries below are planned and run from PySpark, with the rule off and then on.
path = jvm.spike.DbxProbe.prepareOne(jspark, base, "parquet", nrows)
spark.read.parquet(path).createOrReplaceTempView("t")
full = spark.table("t").schema.simpleString()
checks = []
for prefix in ["1b", "2c", "3b", "4b"]:
    sql = jvm.spike.DbxProbe.caseSql(prefix)
    jvm.spike.LambdaPruning.uninstall(jspark)
    off_rows = sorted(str(r) for r in spark.sql(sql).collect())
    off = json.loads(jvm.spike.DbxProbe.describeDataset(spark.sql(sql)._jdf, full))
    jvm.spike.LambdaPruning.install(jspark)
    on_rows = sorted(str(r) for r in spark.sql(sql).collect())
    on = json.loads(jvm.spike.DbxProbe.describeDataset(spark.sql(sql)._jdf, full))
    checks.append({"case": prefix, "equal": off_rows == on_rows, "rows": len(on_rows),
                   "readOff": off["reads"], "readOn": on["reads"],
                   "scanClasses": on["scanClasses"], "counters": on["counters"]})
    print("py4j", prefix, checks[-1], flush=True)
report["pythonChecks"] = checks
jvm.spike.LambdaPruning.uninstall(jspark)
save("python-check", report)

# The Scala matrix (installs the rule itself, from Scala, for each rule-on run).
if mode in ("smoke", "full"):
    kinds = "parquet" if mode == "smoke" else "parquet,delta-dv,delta-colmap,delta-liquid,delta-all"
    only = "1b,2c,4b" if mode == "smoke" else ""
    jvm.spike.LambdaPruning.reset()
    res = jvm.spike.DbxProbe.runMatrix(jspark, base, out, kinds, nrows, only)
    print("MATRIX-DONE", len(res), flush=True)

# Case 5: the engine views, on the cluster that has the Pathling jars.
if mode == "engine":
    # Which expression classes make the rule demand whole columns, for the cases that do not prune.
    jvm.spike.LambdaPruning.reset()
    jvm.spike.DbxProbe.runMatrix(jspark, base, out + "-diag", "parquet", nrows, "3a,3c")
    for variant in ["parquet", "delta-dv"]:
        try:
            r = jvm.spike.EngineViews.run(jspark, "dbfs:/lpr/ndjson", "dbfs:/lpr/small", base, out, variant)
            print("ENGINE-DONE", variant, len(r), flush=True)
        except Exception:
            print("ENGINE-FAILED", variant, traceback.format_exc()[-3000:], flush=True)
            save(f"engine-{variant}-error", {"error": traceback.format_exc()})
