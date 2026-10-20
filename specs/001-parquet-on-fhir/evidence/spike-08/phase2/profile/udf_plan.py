"""Counts the calls of B's UDF in Spark's optimized and physical plans for Observation, with the UDF marked
non-deterministic and without, and times one encode of each at size s, to show whether Spark copies the call
into each top-level column it is expanded into."""
import re
import sys
import time

sys.path.insert(0, sys.argv[1] + "/scripts")
from pathling import PathlingContext  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402

pc = PathlingContext.create(enable_terminology=False, enable_extensions=True)
spark = pc.spark
spark.sparkContext.setLogLevel("ERROR")
jvm = spark._jvm
data = sys.argv[2]
raw = spark.read.text(f"{data}/Observation.ndjson").cache()
raw.count()
docs = getattr(raw._jdf, "as")(jvm.org.apache.spark.sql.Encoders.STRING())
ctx = pc._jpc.getFhirEncoders().getContext()

for nondeterministic in (True, False):
    df = DataFrame(jvm.au.csiro.pathling.io.Spike08BUdf.fit(docs, ctx, "Observation", "fail", False,
                                                           nondeterministic), spark)
    qe = df._jdf.queryExecution()
    optimized, physical = qe.optimizedPlan().toString(), qe.executedPlan().toString()
    calls = lambda plan: len(re.findall(r"Parse\(|UDF\(", plan))
    df.write.format("noop").mode("overwrite").save()
    t = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    ms = (time.perf_counter() - t) * 1000
    print(f"nondeterministic={nondeterministic}: columns {len(df.columns)}, UDF calls in optimized plan "
          f"{calls(optimized)}, in physical plan {calls(physical)}, encode {ms:.0f} ms")
    print("  optimized plan, first lines:")
    for line in optimized.splitlines()[:4]:
        print("   ", line[:220])
