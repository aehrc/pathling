"""The StAX narrative pass with Spark's from_xml and to_xml, on both sides.

Usage: python -I narrative_stax.py <library-runtime jar> <slf4j-api jar> <scratch dir>

Java supplies the pre-pass, the post-pass and the HAPI check (NarrativeCheck.java, beside this
file); Spark supplies from_xml and to_xml. Spark runs without the library-runtime jar, whose
Woodstox clashes with to_xml (see xml-native.md). Needs PySpark 4.0.
"""

import base64
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession

jar, slf4j, scratch = sys.argv[1], sys.argv[2], Path(sys.argv[3])
here = Path(__file__).parent
cp = f"{jar}:{slf4j}"


def java(*args: str) -> str:
    return subprocess.run(
        ["java", "-cp", cp, str(here / "NarrativeCheck.java"), *args],
        check=True, capture_output=True, text=True,
    ).stdout


def b64d(s: str) -> str:
    return base64.b64decode(s).decode("utf-8")


def b64e(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


dump = scratch / "dump.tsv"
java("dump", str(dump))
cases = [line.split("\t") for line in dump.read_text().splitlines()]
# Patients with a narrative of their own, so that they fit one fixed schema. Bundles and contained
# resources have other shapes and are covered by NarrativeCheck.java edges.
rows = [
    (n, b64d(rw), b64d(cap), b64d(orig))
    for n, rw, cap, orig in cases
    if cap and b64d(orig).startswith("<Patient") and "contained" not in n
]

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
spark.createDataFrame(
    rows, "name STRING, rewritten STRING, captured STRING, original STRING"
).createOrReplaceTempView("cases")
SCHEMA = (
    "STRUCT<id: STRUCT<_value: STRING>, text: STRUCT<status: STRUCT<_value: STRING>, "
    "div: STRUCT<_value: STRING>>>"
)
spark.sql(
    f"CREATE OR REPLACE TEMPORARY VIEW parsed AS SELECT name, captured, original, "
    f"from_xml(rewritten, '{SCHEMA}') AS p FROM cases"
)

print("== 1. Ingress: from_xml reads the rewritten narrative as an ordinary primitive")
bad = 0
for r in spark.sql("SELECT name, captured, p FROM parsed").collect():
    ok = r.p.text.div._value == r.captured
    bad += not ok
    print(f"   {r.name:42} identical to the captured string: {ok}")
print("   ALL IDENTICAL" if not bad else f"   {bad} DIFFER")

print("== 2. Egress: to_xml writes the narrative as an attribute, the post-pass restores it")
written = spark.sql(
    "SELECT name, original, to_xml(named_struct('id', p.id, 'text', p.text, "
    "'_xmlns', 'http://hl7.org/fhir'), map('rowTag', 'Patient', 'indent', '')) AS xml FROM parsed"
).collect()
print("   sample to_xml output:", written[0].xml.strip()[:260])
out = scratch / "spark-written.tsv"
out.write_text(
    "\n".join(f"{r.name}\t{b64e(r.xml.strip())}\t{b64e(r.original)}" for r in written) + "\n"
)
print(java("verify", str(out)).rstrip())
