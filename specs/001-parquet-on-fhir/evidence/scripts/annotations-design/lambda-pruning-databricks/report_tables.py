"""Prints the per-case tables of the write-up from the result JSON. Usage: report_tables.py"""
import json

R = "results"
M = "matrix-parquet+delta-dv+delta-colmap+delta-liquid+delta-all.json"


def mb(b):
    return f"{b / 1e6:.2f}"


def short(reads):
    return reads[0].replace(" (whole)", "").replace("_databricks_internal_edge_computed_column_skip_row (not in table schema)", "+dv-skip-col")[:78]


for kind in ("parquet", "delta-all"):
    print(f"\n### Table kind `{kind}`: rule off, rule on, and whether our rule fired\n")
    print("| Case | Off: read | Off MB | On, Photon off: read | MB | Fired | On, Photon on: read | MB | Fired |")
    print("| --- | --- | ---: | --- | ---: | --- | --- | ---: | --- |")
    a = next(k for k in json.load(open(f"{R}/full2-nophoton/{M}"))["results"] if k["kind"] == kind)["cases"]
    b = next(k for k in json.load(open(f"{R}/full2-photon/{M}"))["results"] if k["kind"] == kind)["cases"]
    for x, y in zip(a, b):
        def fired(c):
            n = c["on"]["ruleCounters"]
            return "rewrote" if n[0] else ("**bailed**" if n[3] else "no change")
        print(f"| {x['case'][:3].strip()} | {short(x['off']['info']['reads'])} | {mb(x['off']['bytesRead'])} | "
              f"{short(x['on']['info']['reads'])} | {mb(x['on']['bytesRead'])} | {fired(x)} | "
              f"{short(y['on']['info']['reads'])} | {mb(y['on']['bytesRead'])} | {fired(y)} |")

print("\n### Engine views (parquet)\n")
print("| View | Off: read | Off MB | On, Photon off: read | MB | Fired | On, Photon on: read | MB | Fired |")
print("| --- | --- | ---: | --- | ---: | --- | --- | ---: | --- |")
a = json.load(open(f"{R}/engine3-nophoton/engine-parquet.json"))["views"]
b = json.load(open(f"{R}/engine3-photon/engine-parquet.json"))["views"]
for x, y in zip(a, b):
    def fired(c):
        n = c["on"]["ruleCounters"]
        return "rewrote" if n[0] else ("**bailed**" if n[3] else "no change")
    print(f"| {x['case'][:60].replace('|', chr(92) + '|')} | {short(x['off']['info']['reads'])} | {mb(x['off']['bytesRead'])} | "
          f"{short(x['on']['info']['reads'])} | {mb(x['on']['bytesRead'])} | {fired(x)} | "
          f"{short(y['on']['info']['reads'])} | {mb(y['on']['bytesRead'])} | {fired(y)} |")
