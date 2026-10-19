"""Spike 08: prints the phase table (fit, encode, decode, and the Parquet scan control) for labels run by
run-phases.sh, with the ratio of each label to the first, taken on the minimum of five samples.
Usage: compare_phases.py <baseline-label> <label> [<label> ...]   (labels are directories under results/)
A source a label did not run (from_xml has no JSON) is left blank. For the from_xml route, the py4j
plan-building part of fit and decode is printed as well, as "fit-py4j" and "decode-py4j" (see fromxml_route.py).
"""
import glob, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
labels = sys.argv[1:]
TYPES = ["Patient", "Condition", "Encounter", "Observation"]
SOURCES = ["json", "xml-lines", "xml-bundles", "xml-bundles-txn"]
PHASES = [("fit", "fitMs"), ("encode", "encodeMs"), ("decode", "decodeMs"), ("scan", "parquetScanControlMs")]


def load(label):
    out = {}
    for f in glob.glob(f"{HERE}/results/{label}/phases-*.json"):
        d = json.load(open(f))
        out[(d["source"], int(d["enableExtensions"]))] = d["results"]
    return out


data = {l: load(l) for l in labels}
head = f"{'source':16} {'ext':3} {'type':12} {'phase':12}" + "".join(
    f" {l[:30]:>30} mean     min" + ("  ratio" if i else "") for i, l in enumerate(labels))
print(head)
print("-" * len(head))
for src in SOURCES:
    for ext in (0, 1):
        for t in TYPES:
            phases = list(PHASES) + [("fit-py4j", "fitPlanPy4jMs"), ("decode-py4j", "decodePlanPy4jMs")]
            for name, key in phases:
                cells, base, any_value = [], None, False
                for i, l in enumerate(labels):
                    r = data[l].get((src, ext), {}).get(t, {}).get(key)
                    if r is None or r.get("mean") is None:
                        cells.append(f" {'':>30} {'-':>4} {'-':>7}" + ("       " if i else ""))
                        continue
                    any_value = True
                    mn = r["min"]
                    if i == 0:
                        base = mn
                    ratio = f"  {mn / base:4.2f}x" if i and base else ("       " if i else "")
                    cells.append(f" {'':>22}{r['mean']:8,.0f} {mn:7,.0f}" + ratio)
                if any_value:
                    print(f"{src:16} {ext:<3} {t:12} {name:12}" + "".join(cells))
print()
print("Fidelity (decoded against input, first 500 rows) and rows")
for src in SOURCES:
    for ext in (0, 1):
        for t in TYPES:
            row = []
            for l in labels:
                r = data[l].get((src, ext), {}).get(t)
                if r is None:
                    row.append(f"{'-':>34}")
                    continue
                canon = f" canon {r['fidelityCanonicalEqual']}" if "fidelityCanonicalEqual" in r else ""
                row.append(f"{r['rowsIn']:>8}/{r['rowsEncoded']:>8}/{r['rowsDecoded']:>8} exact {r['fidelityExactEqual']}/{r['fidelitySample']}{canon}".rjust(34))
            print(f"{src:16} {ext:<3} {t:12} " + " | ".join(row))
print()
print("Spark jobs per phase (fit, encode, decode), first sample")
for l in labels:
    seen = sorted({(k[0], tuple(v["jobs"][p][0] for p in ("fit", "encode", "decode"))) for k, rs in data[l].items() for v in rs.values()})
    print(f"  {l}: {seen}")
