"""With an inferred schema (full scan), are the silent cases of corrupt_probe still silent?"""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
def run(label, rows):
    df = spark.read.option("mode", "PERMISSIVE").json(spark.sparkContext.parallelize(rows))
    print(f"== {label}\n   schema: {df.schema.simpleString()}")
    for r in df.collect():
        print("  ", json.dumps(r.asDict(recursive=True)))
run("array for a string, mixed with strings", ['{"id":"1","family":"Smith"}', '{"id":"2","family":["A","B"]}'])
run("array for a string, in every row", ['{"id":"1","family":["A"]}', '{"id":"2","family":["A","B"]}'])
run("object for a string, mixed", ['{"id":"1","family":"Smith"}', '{"id":"2","family":{"x":1}}'])
run("number for a string, mixed", ['{"id":"1","family":"Smith"}', '{"id":"2","family":1.50}'])
run("duplicate key", ['{"id":"1","gender":"male","gender":"female"}'])
run("top-level array", ['{"id":"1"}', '[{"id":"2a"},{"id":"2b"}]'])
run("trailing text", ['{"id":"1"} xyz'])
