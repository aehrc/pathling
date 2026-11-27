"""With an explicit schema in PERMISSIVE mode, which fields of a row with one mismatched value survive?"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
schema = StructType([
    StructField("id", StringType()),
    StructField("valueInteger", IntegerType()),
    StructField("status", StringType()),
    StructField("name", ArrayType(StructType([StructField("given", ArrayType(StringType())), StructField("family", StringType())]))),
    StructField("_corrupt_record", StringType()),
])
rows = [
    '{"id":"bad-first","valueInteger":"5","status":"final","name":[{"family":"X"}]}',
    '{"id":"bad-last","status":"final","name":[{"family":"X"}],"valueInteger":"5"}',
    '{"valueInteger":"5","id":"bad-before-id","status":"final"}',
    '{"id":"bad-nested","status":"final","name":[{"given":"Ann","family":"X"}]}',
]
for opts in [{}, {"spark.sql.json.enablePartialResults": "false"}]:
    for k, v in opts.items():
        spark.conf.set(k, v)
    print("==", opts or "defaults")
    df = spark.read.schema(schema).option("mode", "PERMISSIVE").json(spark.sparkContext.parallelize(rows))
    for r in df.collect():
        d = r.asDict(recursive=True); d["_corrupt_record"] = "<raw line>" if d["_corrupt_record"] else None
        print("  ", json.dumps(d))
