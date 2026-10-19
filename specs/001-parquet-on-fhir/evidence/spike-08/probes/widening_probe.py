"""Does schema inference widen an ancestor to string when two rows disagree deep inside an array or a struct?

A+ reads with the inferred schema and resolves a disagreeing path per value with from_json. Where a
disagreement widens only the leaf, the cost is one from_json per leaf. Where it widens an ancestor, every row
pays a from_json of the whole subtree. Each case below is two documents that agree everywhere but at one
place. The output gives the inferred type of the top-level field and of the path to the disagreement.
"""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
print("spark", spark.version)

cases = [
    ("leaf: string vs number, in a struct in an array",
     '{"name":[{"family":"A","period":{"start":"2020"}}]}', '{"name":[{"family":"B","period":{"start":5}}]}'),
    ("leaf: string vs boolean, in a struct in an array",
     '{"name":[{"family":"A","use":"x"}]}', '{"name":[{"family":"B","use":true}]}'),
    ("leaf: number vs boolean, deep",
     '{"component":[{"valueQuantity":{"value":1.5,"unit":"mg"}}]}', '{"component":[{"valueQuantity":{"value":true,"unit":"mg"}}]}'),
    ("leaf: string vs object, deep in an array of structs",
     '{"code":{"coding":[{"system":"s","code":"c"}]}}', '{"code":{"coding":[{"system":"s","code":{"x":1}}]}}'),
    ("leaf: string vs array (c5-array-for-string-primitive)",
     '{"name":[{"family":"A","given":["x"]}]}', '{"name":[{"family":["A","B"],"given":["y"]}]}'),
    ("array element: string vs number",
     '{"name":[{"given":["a"]}]}', '{"name":[{"given":[1]}]}'),
    ("array element: string vs object",
     '{"name":[{"given":["a"]}]}', '{"name":[{"given":[{"x":1}]}]}'),
    ("array element: string vs nested array (c5-nested-array)",
     '{"name":[{"given":["a"]}]}', '{"name":[{"given":[["a","b"]]}]}'),
    ("intermediate: array of structs vs struct (c5-scalar-for-array)",
     '{"name":[{"family":"X","given":["Ann"]}],"gender":"male"}', '{"name":{"family":"Y","given":["Bo"]},"gender":"female"}'),
    ("intermediate: struct vs array of structs, one level down",
     '{"code":{"coding":[{"system":"s","code":"c"}],"text":"t"}}', '{"code":{"coding":{"system":"s","code":"c"},"text":"t"}}'),
    ("intermediate: struct vs string",
     '{"code":{"coding":[{"system":"s"}],"text":"t"}}', '{"code":"just text"}'),
    ("intermediate: array of structs vs array of strings",
     '{"name":[{"family":"X"}]}', '{"name":["X"]}'),
    ("intermediate: struct vs empty object, and vs empty array",
     '{"code":{"text":"t"}}', '{"code":{}}'),
    ("intermediate: array of structs vs empty array",
     '{"name":[{"family":"X"}]}', '{"name":[]}'),
    ("intermediate: struct vs null",
     '{"code":{"text":"t"}}', '{"code":null}'),
]


def walk(t, path=""):
    """Yields (path, type) for every node of the type, so the widened ancestor is visible."""
    from pyspark.sql.types import StructType, ArrayType
    yield path or "<root>", t.simpleString() if len(t.simpleString()) < 90 else t.simpleString()[:87] + "..."
    if isinstance(t, StructType):
        for f in t.fields:
            yield from walk(f.dataType, f"{path}.{f.name}" if path else f.name)
    elif isinstance(t, ArrayType):
        yield from walk(t.elementType, path + "[]")


for label, a, b in cases:
    print(f"\n== {label}\n   row 1: {a}\n   row 2: {b}")
    for docs, which in (([a], "row 1 alone"), ([b], "row 2 alone"), ([a, b], "both rows")):
        df = spark.read.json(spark.createDataFrame([(d,) for d in docs], "j string").rdd.map(lambda r: r.j))
        print(f"   {which}:")
        for p, t in list(walk(df.schema))[1:]:
            print(f"      {p:34} {t}")
        if which == "both rows":
            for r in df.collect():
                print("      row:", json.dumps(r.asDict(recursive=True), default=str))
