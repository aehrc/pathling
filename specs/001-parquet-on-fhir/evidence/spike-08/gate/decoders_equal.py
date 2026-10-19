"""Are the from_xml route's decoder and HEAD's XML decoder byte-identical on the same stored data (size s)?

The gate compared each decoder's output with the input; this compares the two outputs with each other, by
resource id, as strings. The stored data is HEAD's own encoding of the XML lines, so both decoders read the
same rows. Usage: decoders_equal.py <harnessScriptsDir> <xmlDir> <Type> [...]
"""
import os, re, sys
from pyspark.sql import DataFrame
from pathling import PathlingContext

scripts, xml_dir, *types = sys.argv[1:]
sys.path.insert(0, scripts)
from fromxml_route import XmlRoute

XML = "application/fhir+xml"
ID = re.compile(r'<id value="([^"]+)"/>')
pc = PathlingContext.create(enable_terminology=False)
spark = pc.spark
spark.sparkContext.setLogLevel("OFF")
route = XmlRoute(pc)
for t in types:
    stored = pc.encode(spark.read.text(f"{xml_dir}/{t}.ndxml"), t, input_type=XML).cache()
    ours = {ID.search(r[0]).group(1): r[0] for r in route.decode(stored, t).collect()}
    head = {ID.search(r[0]).group(1): r[0] for r in DataFrame(pc._jpc.decode(stored._jdf, t, XML), spark).collect()}
    differ = [i for i in head if ours.get(i) != head[i]]
    print(f"{t:12} documents {len(head)}  from_xml decoder {len(ours)}  byte-identical {len(head) - len(differ)}  differ {len(differ)}")
    for i in differ[:3]:
        a, b = head[i], ours.get(i, "")
        n = next((k for k in range(min(len(a), len(b))) if a[k] != b[k]), min(len(a), len(b)))
        print(f"   {i}: HEAD ...{a[max(0, n - 40):n + 60]!r}\n   {'':{len(i)}}  ours ...{b[max(0, n - 40):n + 60]!r}")
    stored.unpersist()
