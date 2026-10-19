"""Spike 08: the range over the four types of label/baseline ratios (minimum of five samples), per source,
extensions setting and phase. Usage: summary_phases.py <baseline-label> <label>"""
import glob, json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def load(label):
    out = {}
    for f in glob.glob(f"{HERE}/results/{label}/phases-*.json"):
        d = json.load(open(f)); out[(d["source"], int(d["enableExtensions"]))] = d["results"]
    return out
base, other = load(sys.argv[1]), load(sys.argv[2])
print(f"{'source':16} {'ext':3} " + "  ".join(f"{p:>13}" for p in ("fit", "encode", "decode", "scan")))
for src in ("json", "xml-lines", "xml-bundles", "xml-bundles-txn"):
    for ext in (0, 1):
        if (src, ext) not in base or (src, ext) not in other:
            continue
        cells = []
        for key in ("fitMs", "encodeMs", "decodeMs", "parquetScanControlMs"):
            rs = [other[(src, ext)][t][key]["min"] / base[(src, ext)][t][key]["min"] for t in base[(src, ext)]]
            cells.append(f"{min(rs):.2f}-{max(rs):.2f}".rjust(13))
        print(f"{src:16} {ext:<3} " + "  ".join(cells))
