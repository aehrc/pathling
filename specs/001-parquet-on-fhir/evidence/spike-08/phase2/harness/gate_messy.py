"""Spike 08 phase 2: what each route stores from the messy data (tools/make_messy.py), against HEAD's encoding of
the clean data. One document per file has a shape error at an intermediate node, so a route that keeps the
granularity of a contradicting value differs from the clean encoding in that one row only; a route that loses
the column differs in every row that carries it. Candidates run under SPIKE_POLICY=drop (fail would stop).
Usage: gate_messy.py <route|pathling> <cleanJsonDir> <messyJsonDir> <Type> [...]
"""
import json, os, re, sys
from pathling import PathlingContext

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("SPIKE_POLICY", "drop")
from spike_routes import ROUTES

route_name, clean, messy, *types = sys.argv[1:]
pc = PathlingContext.create(enable_terminology=False)
spark = pc.spark
spark.sparkContext.setLogLevel("OFF")
route = ROUTES[route_name](pc) if route_name in ROUTES else None


def norm(x):
    if isinstance(x, dict):
        return {k: norm(v) for k, v in x.items() if v is not None}
    if isinstance(x, list):
        return [norm(v) for v in x]
    return x


def numeric(x):
    """Decimals compared by value: HEAD stores a double's text, a candidate the source's."""
    if isinstance(x, dict):
        return {k: numeric(v) for k, v in x.items()}
    if isinstance(x, list):
        return [numeric(v) for v in x]
    if isinstance(x, str) and re.fullmatch(r"-?[0-9.]+([eE][+-]?[0-9]+)?", x):
        try:
            return float(x)
        except ValueError:
            return x
    return x


for t in types:
    ref = {r["id"]: numeric(norm(r.asDict(recursive=True))) for r in pc.encode(spark.read.text(f"{clean}/{t}.ndjson"), t).collect()}
    raw = spark.read.text(f"{messy}/{t}.ndjson")
    encoded = route.fit("json", raw, t) if route else pc.encode(raw, t)
    got = {}
    findings = []
    for r in encoded.collect():
        d = r.asDict(recursive=True)
        findings += d.pop("_findings", None) or []
        got[d["id"]] = numeric(norm(d))
    differ = [i for i in ref if ref[i] != got.get(i)]
    keys = sorted({k for i in differ for k in set(ref[i]) | set(got.get(i, {})) if ref[i].get(k) != got.get(i, {}).get(k)})
    print(f"{route_name:9} {t:12} rows {len(ref)} vs {len(got)}  differing rows {len(differ)}  in {keys}  findings {len(findings)}"
          + (f"  e.g. {findings[0]}" if findings else ""), flush=True)
