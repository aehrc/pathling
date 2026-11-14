# Decision 68: a decimal is read through a double, stored as text, and written
# through a double again. What text does each step produce?
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

DOCS = [
    '{"id":"trailing-zero","v":1.50}',
    '{"id":"integral","v":100}',
    '{"id":"small","v":0.0000001}',
    '{"id":"large","v":1e20}',
    '{"id":"exponent-written","v":1.5e2}',
    '{"id":"forty-digits","v":1234567890123456789012345678901234567890.5}',
    '{"id":"negative","v":-2.5}',
]
rdd = spark.sparkContext.parallelize(DOCS)

print("1. inference over all documents (mixed), no primitivesAsString:")
d = spark.read.json(rdd)
print("   schema:", d.schema.simpleString())

print("2. ingest: cast(inferred as string) -- this is what is stored:")
stored = d.select("id", col("v").cast(StringType()).alias("v"))
rows = {r["id"]: r["v"] for r in stored.collect()}
for k in [r.split('"')[3] for r in DOCS]:
    print(f"   {k:20s} -> {rows[k]!r}")

print("3. egress: cast(stored as double), rendered by the JSON writer:")
out = stored.select("id", to_json(struct(col("v").cast(DoubleType()).alias("v"))).alias("doc"))
for r in out.collect():
    print(f"   {r['id']:20s} -> {r['doc']}")

print("4. an all-integral file infers LongType, not DoubleType:")
integral = spark.read.json(spark.sparkContext.parallelize(['{"v":100}', '{"v":7}']))
print("   schema:", integral.schema.simpleString())
print("   stored:", [r["v"] for r in integral.select(col("v").cast(StringType())).collect()])
print("   egress:", [r["doc"] for r in integral.select(
    to_json(struct(col("v").cast(DoubleType()).alias("v"))).alias("doc")).collect()])

print("5. try_cast(long as int) on overflow, which is how an out-of-range integer is ignored:")
big = spark.sql("SELECT try_cast(3000000000L AS INT) AS a, try_cast(7L AS INT) AS b")
print("   ", big.collect()[0].asDict())
