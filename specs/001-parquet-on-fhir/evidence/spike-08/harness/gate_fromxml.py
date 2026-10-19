"""Correctness gate for the from_xml route (spike 08), run at size s before anything is timed.

For each resource type, the dataset the from_xml route encodes from XML lines, collection Bundles and
transaction Bundles is compared with the dataset HEAD encodes from the JSON source (pc.encode), in schema and
in rows. HEAD's own HAPI XML route is compared the same way, as a reference. Decimals are compared by value,
and a decimal whose text differs is counted separately: the JSON reader infers integral columns as bigint, so
the JSON route can store "0" where an XML route stores "0.0" (OQ-1's decimal loss, which A0 accepts).
Then the from_xml route's decoded XML is compared with the input, exactly and after canonicalisation.
Usage: gate_fromxml.py <jsonDir> <xmlDir> <bundleDir> <txnBundleDir> <Type> [...]
"""
import json, os, re, sys
import xml.etree.ElementTree as ET
from pyspark.sql import DataFrame
from pathling import PathlingContext

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fromxml_route import XmlRoute

json_dir, xml_dir, bundle_dir, txn_dir, *types = sys.argv[1:]
XML = "application/fhir+xml"
ID = re.compile(r'<id value="([^"]+)"/>')
pc = PathlingContext.create(enable_terminology=False, enable_extensions=os.environ.get("ENABLE_EXTENSIONS") == "1")
spark = pc.spark
spark.sparkContext.setLogLevel("ERROR")
route = XmlRoute(pc)


def as_dicts(df):
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
        if isinstance(a, str) and isinstance(b, str):
            try:
                if float(a) == float(b):
                    kind = "decimal-text"
            except ValueError:
                pass
        out.append((kind, re.sub(r"\[\d+\]", "[]", path), a, b))


def compare(label, reference, candidate):
    same_schema = reference.schema == candidate.schema
    ref, cand = as_dicts(reference), as_dicts(candidate)
    diffs = []
    for i in sorted(set(ref) | set(cand)):
        diff(ref.get(i), cand.get(i), "", diffs)
    by = {}
    for kind, path, a, b in diffs:
        by.setdefault((kind, path), []).append((a, b))
    print(f"   {label:28} rows {len(ref)} vs {len(cand)}  schema {'equal' if same_schema else 'DIFFERS'}  "
          f"value diffs {sum(1 for d in diffs if d[0] == 'value')}  decimal-text diffs {sum(1 for d in diffs if d[0] == 'decimal-text')}")
    if not same_schema:
        r, c = reference.schema.simpleString(), candidate.schema.simpleString()
        n = next((i for i in range(min(len(r), len(c))) if r[i] != c[i]), min(len(r), len(c)))
        print(f"      schema first differs at {n}: ref ...{r[max(0, n-80):n+120]}")
        print(f"                                   cand ...{c[max(0, n-80):n+120]}")
    for (kind, path), pairs in sorted(by.items())[:12]:
        print(f"      {kind:12} {path}  x{len(pairs)}  e.g. {str(pairs[0][0])[:90]!r} -> {str(pairs[0][1])[:90]!r}")


def one_line(doc):
    return doc.replace("\r", "&#xd;").replace("\n", "&#xa;")


bundles = spark.read.text(bundle_dir, wholetext=True).cache()
txn = spark.read.text(txn_dir, wholetext=True).cache()
for t in types:
    print(f"\n== {t}")
    js = pc.encode(spark.read.text(f"{json_dir}/{t}.ndjson"), t)
    lines = spark.read.text(f"{xml_dir}/{t}.ndxml").cache()
    compare("HAPI xml lines (HEAD)", js, pc.encode(lines, t, input_type=XML))
    fx = route.fit_lines(lines, t)
    print(f"   undescribed dropped by the from_xml prune: {sorted(set(route.undescribed))}")
    compare("from_xml lines", js, fx)
    compare("from_xml bundles", js, route.fit_bundles(bundles, t))
    compare("from_xml txn bundles", js, route.fit_bundles(txn, t))
    # Egress: the from_xml route's XML against the input lines, and HEAD's XML for reference.
    orig = {ID.search(r.value).group(1): r.value for r in lines.collect()}
    for label, out in (("from_xml decode", route.decode(fx, t)),
                       ("HEAD decode of from_xml data", DataFrame(pc._jpc.decode(fx._jdf, t, XML), spark))):
        back = [r[0] for r in out.collect()]
        exact = sum(1 for d in back if orig.get(ID.search(d).group(1)) == one_line(d))
        canon = sum(1 for d in back if ET.canonicalize(xml_data=orig[ID.search(d).group(1)]) == ET.canonicalize(xml_data=d))
        print(f"   {label:28} exact {exact}/{len(back)}  canonical {canon}/{len(back)}")
        bad = [d for d in back if ET.canonicalize(xml_data=orig[ID.search(d).group(1)]) != ET.canonicalize(xml_data=d)]
        if bad:
            a, b = ET.canonicalize(xml_data=orig[ID.search(bad[0]).group(1)]), ET.canonicalize(xml_data=bad[0])
            n = next((i for i in range(min(len(a), len(b))) if a[i] != b[i]), min(len(a), len(b)))
            print(f"      first canonical difference at {n}: in ...{a[max(0, n-60):n+80]!r}\n                                     out ...{b[max(0, n-60):n+80]!r}")
    lines.unpersist()
