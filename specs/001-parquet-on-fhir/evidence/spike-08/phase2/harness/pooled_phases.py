"""Spike 08: ratios on the minimum of ten samples per label, pooling the extensions-off and extensions-on runs.
Neither 26d7332 nor 514f8f8 reads the extensions flag on these paths, so the two runs are replicates of the same
code. Prints the pooled minimum of each label and the ratio, per source, type and phase, and the range per
source and phase. Usage: pooled_phases.py <baseline-label> <label> [--py4j]
--py4j subtracts the label's py4j plan-building time (from_xml only) from fit and decode, sample by sample."""
import glob, json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
args = [a for a in sys.argv[1:] if not a.startswith("--")]
strip = "--py4j" in sys.argv
def load(label):
    out = {}
    for f in glob.glob(f"{HERE}/results/{label}/phases-*.json"):
        d = json.load(open(f)); out.setdefault(d["source"], []).append(d["results"])
    return out
def pooled(runs, t, key, sub=None):
    xs = []
    for r in runs:
        s = r[t][key]["samples"]
        if sub and sub in r[t]:
            s = [a - b for a, b in zip(s, r[t][sub]["samples"])]
        xs += s
    return min(xs)
base, other = load(args[0]), load(args[1])
print(f"{'source':16} {'type':12} " + "  ".join(f"{p:>22}" for p in ("fit", "encode", "decode")))
ranges = {}
for src in ("json", "xml-lines", "xml-bundles", "xml-bundles-txn"):
    if src not in base or src not in other: continue
    for t in ("Patient", "Condition", "Encounter", "Observation"):
        cells = []
        for key, sub in (("fitMs", "fitPlanPy4jMs"), ("encodeMs", None), ("decodeMs", "decodePlanPy4jMs")):
            a = pooled(base[src], t, key); b = pooled(other[src], t, key, sub if strip else None)
            ranges.setdefault((src, key), []).append(b / a)
            cells.append(f"{a:8,.0f} {b:8,.0f} {b/a:4.2f}")
        print(f"{src:16} {t:12} " + "  ".join(cells))
print()
for (src, key), rs in ranges.items():
    print(f"{src:16} {key[:-2]:7} {min(rs):.2f}-{max(rs):.2f}")
