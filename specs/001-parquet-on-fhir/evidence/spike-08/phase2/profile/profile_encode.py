"""Profiles a route's encode (B by default; pathling for A0, orig for the original spike) of Observation at size l with Java Flight Recorder, recording only the encode runs."""
import subprocess
import sys
import time

sys.path.insert(0, sys.argv[1] + "/scripts")
from pathling import PathlingContext  # noqa: E402
from spike_routes import ROUTES  # noqa: E402

out_file, runs = sys.argv[2], int(sys.argv[3])
route_name = sys.argv[4] if len(sys.argv) > 4 else "b"
pc = PathlingContext.create(enable_terminology=False, enable_extensions=True)
spark = pc.spark
spark.sparkContext.setLogLevel("ERROR")
route = ROUTES[route_name](pc) if route_name != "pathling" else None
data = "/Users/szu004/dev/sql-on-fhir.js/benchmark/data/synthea-clinical-wide/1/l"
raw = spark.read.text(f"{data}/Observation.ndjson").cache()
print("rows", raw.count(), flush=True)
encoded = route.fit("json", raw, "Observation") if route else pc.encode(raw, "Observation")
encoded.write.format("noop").mode("overwrite").save()  # Warm-up, unrecorded.

pid = str(spark._jvm.java.lang.ProcessHandle.current().pid())
subprocess.run(["jcmd", pid, "JFR.start", "name=enc", "settings=profile"], check=True, capture_output=True)
for i in range(runs):
    t = time.perf_counter()
    encoded.write.format("noop").mode("overwrite").save()
    print(f"encode {i}: {(time.perf_counter() - t) * 1000:.0f} ms", flush=True)
subprocess.run(["jcmd", pid, "JFR.stop", "name=enc", f"filename={out_file}"], check=True, capture_output=True)
print("recorded", out_file, flush=True)
