"""Can a Spark-reader route (option A) attribute findings to a value? Checks null vs absent, and explicit-schema mismatches."""
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
rows = [
    '{"id":"a","active":true}',
    '{"id":"b","active":null}',
    '{"id":"c"}',
    '{"id":"d","name":{}}',
    '{"id":"e","given":[]}',
]
df = spark.read.json(spark.sparkContext.parallelize(rows))
print("== inferred, null vs absent")
for r in df.collect():
    print("  ", json.dumps(r.asDict(recursive=True)))

schema = StructType([
    StructField("id", StringType()),
    StructField("valueInteger", IntegerType()),
    StructField("family", StringType()),
    StructField("given", ArrayType(StringType())),
    StructField("_corrupt_record", StringType()),
])
rows2 = [
    '{"id":"ok","valueInteger":5,"family":"X","given":["A"]}',
    '{"id":"str-for-int","valueInteger":"5"}',
    '{"id":"num-for-string","family":1.50}',
    '{"id":"scalar-for-array","given":"A"}',
    '{"id":"overflow","valueInteger":99999999999}',
    '{"id":"unknown-key","foo":"bar"}',
]
df2 = spark.read.schema(schema).option("mode", "PERMISSIVE").json(spark.sparkContext.parallelize(rows2))
print("== explicit schema, PERMISSIVE with _corrupt_record")
for r in df2.collect():
    print("  ", json.dumps(r.asDict(recursive=True)))
