"""Prints the CACHE sizes and a markdown results table from results.txt.
Usage: table.py <results file>."""
import sys

R, C = {}, {}
for line in open(sys.argv[1]):
    d = dict(kv.split("=", 1) for kv in line.split()[1:] if "=" in kv)
    if line.startswith("CACHE"):
        C[(d["source"], d["variant"], d["table"])] = d
    elif line.startswith("RESULT"):
        R[(d["source"], d["expr"], d["arm"], d["variant"])] = (
            float(d["median"]), float(d["min"]), float(d["max"]))
for k, v in sorted(C.items()):
    print(k, v["rows"], v["mem_mb"], v["cached_partitions"])
print("| source | expr | arm | plain | ext | ext - plain (s) | (ext - plain)/plain |")
print("| --- | --- | --- | --- | --- | --- | --- |")
for s in ("cache", "json"):
    for e in ("E1", "E2", "E3"):
        for a in ("bare", "A0", "A1", "L1", "D3a"):
            if (s, e, a, "plain") in R:
                p, x = R[(s, e, a, "plain")], R[(s, e, a, "ext")]
                print("| %s | %s | %s | %.2f (%.2f-%.2f) | %.2f (%.2f-%.2f) | %+.2f | %+.0f%% |"
                      % (s, e, a, p[0], p[1], p[2], x[0], x[1], x[2], x[0] - p[0],
                         (x[0] - p[0]) / p[0] * 100))
