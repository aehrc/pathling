"""Scratch, ticket 32: fit, encode and decode of JSON bundles timed apart, as scripts/phases.py in the
pathling-compare harness times its sources (one warmup, five iterations, input cached untimed, decode from a
Parquet read). The only difference is the source: JSON bundles, one per line, read with pc.encode_bundle.
Usage: json_bundle_phases.py <bundles.ndjson> <outJson> <Type> [...]
"""
import json, statistics, sys, tempfile, time
from pyspark.sql import DataFrame
from pathling import PathlingContext
path, out_file, *resources = sys.argv[1:]
pc = PathlingContext.create(enable_terminology=False)
spark = pc.spark; sc = spark.sparkContext; sc.setLogLevel("ERROR")
def noop(df): df.write.format("noop").mode("overwrite").save()
def phase(group, fn):
    sc.setJobGroup(group, group); t = time.perf_counter(); r = fn(); ms = (time.perf_counter() - t) * 1000
    jobs = len(sc.statusTracker().getJobIdsForGroup(group)); sc.setJobGroup("untimed", "untimed"); return ms, r, jobs
stats = lambda xs: {"mean": statistics.mean(xs), "min": min(xs), "max": max(xs), "samples": xs}
raw = spark.read.text(path).cache(); n = raw.count()
tmp = tempfile.mkdtemp(prefix="jbphases-"); results = {}
for res in resources:
    fits, encodes, decodes = [], [], []
    for i in range(6):
        f, enc, _ = phase(f"fit-{res}-{i}", lambda: pc.encode_bundle(raw, res))
        e, _, _ = phase(f"encode-{res}-{i}", lambda: noop(enc))
        if i: fits.append(f); encodes.append(e)
    enc.write.mode("overwrite").parquet(f"{tmp}/{res}"); stored = spark.read.parquet(f"{tmp}/{res}"); rows = stored.count()
    for i in range(6):
        d, _, _ = phase(f"decode-{res}-{i}", lambda: noop(DataFrame(pc._jpc.decode(stored._jdf, res, "application/fhir+json"), spark)))
        if i: decodes.append(d)
    results[res] = {"bundles": n, "rowsEncoded": rows, "fitMs": stats(fits), "encodeMs": stats(encodes), "decodeMs": stats(decodes)}
    print(res, rows, "fit", round(min(fits)), "encode", round(min(encodes)), "decode", round(min(decodes)), flush=True)
json.dump(results, open(out_file, "w"), indent=1)
