"""Does parse_json (and try_parse_json) reject a duplicate key, a top-level array and text after the object?

Ticket 08 requires every candidate to reject the three things Spark's JSON reader accepts silently. A-variant
builds on parse_json, and A+'s pre-pass could use it as a syntax check, so this records what it does.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
import re
print("spark", spark.version)
for key in ("spark.sql.variant.allowDuplicateKeys", "spark.sql.json.enablePartialResults", "spark.sql.ansi.enabled"):
    print(f"conf {key} = {spark.conf.get(key)}")

cases = [
    ("conformant object", '{"resourceType":"Patient","id":"p1"}'),
    ("duplicate key, top level", '{"resourceType":"Patient","id":"p26","gender":"male","gender":"female"}'),
    ("duplicate key, nested", '{"resourceType":"Patient","name":[{"family":"A","family":"B"}]}'),
    ("top-level array", '[{"resourceType":"Patient","id":"p27a"},{"resourceType":"Patient","id":"p27b"}]'),
    ("text after the object", '{"resourceType":"Patient","id":"p28"} xyz'),
    ("second object after the object", '{"resourceType":"Patient","id":"a"} {"resourceType":"Patient","id":"b"}'),
    ("whitespace after the object", '{"resourceType":"Patient","id":"p28"}   '),
    ("top-level scalar", '"just a string"'),
    ("malformed (unclosed)", '{"resourceType":"Patient","id":"p25"'),
]
spark.createDataFrame(cases, "label string, j string").createOrReplaceTempView("t")


def run(expr, allow_dup):
    spark.conf.set("spark.sql.variant.allowDuplicateKeys", allow_dup)
    print(f"\n== {expr}, spark.sql.variant.allowDuplicateKeys={allow_dup}")
    for label, j in cases:
        try:
            r = spark.sql(f"SELECT {expr}(j) AS v FROM t WHERE label = '{label}'").selectExpr(
                "v IS NULL AS is_null", "schema_of_variant(v) AS schema", "to_json(v) AS json").collect()[0]
            print(f"   {label:32} accepted  null={r.is_null}  schema={r.schema}  json={r.json}")
        except Exception as e:
            # The error classes in the message name the cause; the first line names only the call.
            causes = sorted(set(re.findall(r"\[([A-Z_]+(?:\.[A-Z_]+)?)\]", str(e))))
            eof = "JsonEOFException" if "JsonEOFException" in str(e) else ""
            print(f"   {label:32} REJECTED  {' '.join(causes)} {eof}".rstrip())


for allow in ("false", "true"):
    run("parse_json", allow)
    run("try_parse_json", allow)
