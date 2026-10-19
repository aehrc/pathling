"""Correctness gate for spike 08's candidates (phase 2), run at size s before anything is timed.

For each candidate, source and type: the stored dataset is compared with HEAD's own encoding of the JSON
source (pc.encode), in schema and in every row. A decimal whose text differs but whose value is equal is
counted apart: HEAD stores a double's text, a candidate that keeps the lexical form stores the source's.
Then the candidate's decoded documents are compared with the input: JSON with numbers compared by their
text (lexical) and by value; XML exactly and after canonicalisation. For bundles the reference document is
the resource's line in <xmlDir>/<Type>.ndxml.
Usage: gate_candidates.py <route> <sources,comma-separated> <jsonDir> <xmlDir> <bundleDir> <txnBundleDir> <Type> [...]
"""
import json, os, re, sys
import xml.etree.ElementTree as ET
from pathling import PathlingContext

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spike_routes import ROUTES

route_name, sources, json_dir, xml_dir, bundle_dir, txn_dir, *types = sys.argv[1:]
ID = re.compile(r'<id value="([^"]+)"/>')
pc = PathlingContext.create(enable_terminology=False)
spark = pc.spark
spark.sparkContext.setLogLevel("ERROR")
route = ROUTES[route_name](pc)


def rows(df):
    return {r["id"]: r.asDict(recursive=True) for r in df.collect()}


def diff(a, b, path, out):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            diff(a.get(k), b.get(k), f"{path}.{k}", out)
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            diff(x, y, f"{path}[{i}]", out)
    elif a != b:
        kind = "value"
        try:
            if isinstance(a, str) and isinstance(b, str) and float(a) == float(b):
                kind = "decimal-text"
        except ValueError:
            pass
        out.append((kind, re.sub(r"\[\d+\]", "[]", path), a, b))


def lexical(text):
    return json.loads(text, parse_float=lambda s: ("n", s), parse_int=lambda s: ("n", s))


def canonical(doc):
    return ET.canonicalize(xml_data=doc, rewrite_prefixes=True)


def one_line(doc):
    return doc.replace("\r", "&#xd;").replace("\n", "&#xa;")


raws = {}
for t in types:
    print(f"\n== {t}", flush=True)
    reference = pc.encode(spark.read.text(f"{json_dir}/{t}.ndjson"), t)
    ref = rows(reference)
    for source in sources.split(","):
        if source == "json":
            raw = spark.read.text(f"{json_dir}/{t}.ndjson")
        elif source == "xml-lines":
            raw = spark.read.text(f"{xml_dir}/{t}.ndxml")
        else:
            d = bundle_dir if source == "xml-bundles" else txn_dir
            raw = raws.setdefault(d, spark.read.text(d, wholetext=True).cache())
        encoded = route.fit(source, raw, t)
        got = rows(encoded)
        found = []
        for i in sorted(set(ref) | set(got)):
            diff(ref.get(i), got.get(i), "", found)
        by = {}
        for kind, path, a, b in found:
            by.setdefault((kind, path), []).append((a, b))
        same = encoded.schema == reference.schema
        print(f"   {source:16} rows {len(ref)} vs {len(got)}  schema {'equal' if same else 'DIFFERS'}  "
              f"value diffs {sum(1 for d in found if d[0] == 'value')}  decimal-text diffs {sum(1 for d in found if d[0] == 'decimal-text')}", flush=True)
        if not same:
            a, b = reference.schema.simpleString(), encoded.schema.simpleString()
            n = next((k for k in range(min(len(a), len(b))) if a[k] != b[k]), min(len(a), len(b)))
            print(f"      schema: HEAD ...{a[max(0, n - 60):n + 100]}\n              cand ...{b[max(0, n - 60):n + 100]}")
        for (kind, path), pairs in sorted(by.items())[:8]:
            print(f"      {kind:12} {path} x{len(pairs)} e.g. {str(pairs[0][0])[:60]!r} -> {str(pairs[0][1])[:60]!r}")
        # Egress against the input.
        decoded = [r[0] for r in route.decode(source, encoded, t).collect()]
        if source == "json":
            orig = {json.loads(l)["id"]: l for l in open(f"{json_dir}/{t}.ndjson")}
            lex = sum(1 for d in decoded if lexical(orig[json.loads(d)["id"]]) == lexical(d))
            val = sum(1 for d in decoded if json.loads(orig[json.loads(d)["id"]]) == json.loads(d))
            print(f"      decode: {len(decoded)} documents, equal by value {val}, lexically {lex}")
            bad = [d for d in decoded if lexical(orig[json.loads(d)["id"]]) != lexical(d)][:1]
            for d in bad:
                out = []
                diff(lexical(orig[json.loads(d)["id"]]), lexical(d), "", out)
                print(f"      first lexical difference: {out[:3]}")
        else:
            orig = {ID.search(l).group(1): l.rstrip("\n") for l in open(f"{xml_dir}/{t}.ndxml")}
            exact = sum(1 for d in decoded if orig.get(ID.search(d).group(1)) == one_line(d))
            canon = sum(1 for d in decoded if canonical(orig[ID.search(d).group(1)]) == canonical(d))
            print(f"      decode: {len(decoded)} documents, exact {exact}, canonical {canon}")
            bad = [d for d in decoded if canonical(orig[ID.search(d).group(1)]) != canonical(d)][:1]
            for d in bad:
                a, b = canonical(orig[ID.search(d).group(1)]), canonical(d)
                n = next((k for k in range(min(len(a), len(b))) if a[k] != b[k]), min(len(a), len(b)))
                print(f"      first canonical difference: in ...{a[max(0, n - 50):n + 70]!r}\n                                 out ...{b[max(0, n - 50):n + 70]!r}")
