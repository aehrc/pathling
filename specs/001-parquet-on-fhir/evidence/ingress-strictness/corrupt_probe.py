"""Which inputs put a row into _corrupt_record under an explicit schema in PERMISSIVE mode (Spark 4.0.2)?"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, BooleanType,
                               DoubleType, ArrayType)

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
schema = StructType([
    StructField("id", StringType()),
    StructField("s", StringType()),
    StructField("i", IntegerType()),
    StructField("b", BooleanType()),
    StructField("d", DoubleType()),
    StructField("arr", ArrayType(StringType())),
    StructField("obj", StructType([StructField("x", StringType())])),
    StructField("_corrupt_record", StringType()),
])
cases = [
    ("string: number token", '{"id":"1","s":123}'),
    ("string: boolean token", '{"id":"2","s":true}'),
    ("string: object token", '{"id":"3","s":{"x":1}}'),
    ("string: array token", '{"id":"4","s":["a","b"]}'),
    ("integer: string token", '{"id":"5","i":"5"}'),
    ("integer: decimal token", '{"id":"6","i":5.0}'),
    ("integer: overflow", '{"id":"7","i":99999999999}'),
    ("boolean: string token", '{"id":"8","b":"true"}'),
    ("boolean: number token", '{"id":"9","b":1}'),
    ("double: string token", '{"id":"10","d":"1.5"}'),
    ("double: NaN string", '{"id":"11","d":"NaN"}'),
    ("array: scalar token", '{"id":"12","arr":"a"}'),
    ("array: object token", '{"id":"13","arr":{"x":"a"}}'),
    ("array: nested array", '{"id":"14","arr":[["a","b"]]}'),
    ("array: number element", '{"id":"15","arr":[1,2]}'),
    ("struct: scalar token", '{"id":"16","obj":"a"}'),
    ("struct: array token", '{"id":"17","obj":[{"x":"a"}]}'),
    ("struct: unknown key", '{"id":"18","obj":{"x":"a","y":"b"}}'),
    ("duplicate key", '{"id":"19","s":"first","s":"second"}'),
    ("top-level scalar", '"just a string"'),
    ("top-level array", '[{"id":"21a"},{"id":"21b"}]'),
    ("malformed", '{"id":"22","s":"x"'),
    ("trailing garbage", '{"id":"23"} xyz'),
]
df = spark.read.schema(schema).option("mode", "PERMISSIVE").json(
    spark.sparkContext.parallelize([c for _, c in cases]))
rows = df.collect()
labels = dict((c, l) for l, c in cases)
for r in rows:
    d = r.asDict(recursive=True)
    corrupt = d.pop("_corrupt_record")
    label = labels.get(corrupt, "") if corrupt else ""
    kept = {k: v for k, v in d.items() if v is not None}
    print(f"{'CORRUPT' if corrupt else 'ok     '}  {json.dumps(kept):55} {('<- ' + corrupt) if corrupt else ''}")
