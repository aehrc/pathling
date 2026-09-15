from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled","false")
         .config("spark.sql.shuffle.partitions","1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
DOCS = ['{"id":"a","v":1.50}','{"id":"d","v":1234567890123456789012345678901234567890.5}']
rdd = spark.sparkContext.parallelize(DOCS)
schema = StructType([StructField("id",StringType()),StructField("v",StringType())])

print("1. primitivesAsString=true, inferred schema:")
d = spark.read.option("primitivesAsString","true").json(rdd)
print("   schema:", d.schema.simpleString(), "->", [(r["id"], r["v"]) for r in d.orderBy("id").collect()])

print("2. explicit string schema + primitivesAsString=true:")
d = spark.read.option("primitivesAsString","true").schema(schema).json(rdd)
print("   ->", [(r["id"], r["v"]) for r in d.orderBy("id").collect()])

print("3. prefersDecimal=true, inferred:")
d = spark.read.option("prefersDecimal","true").json(rdd)
print("   schema:", d.schema.simpleString(), "->", [(r["id"], str(r["v"])) for r in d.orderBy("id").collect()])

print("4. enableExactStringParsing explicitly true:")
spark.conf.set("spark.sql.json.enableExactStringParsing","true")
d = spark.read.schema(schema).json(rdd)
print("   ->", [(r["id"], r["v"]) for r in d.orderBy("id").collect()])

print("5. singleVariantColumn:")
try:
    d = spark.read.option("singleVariantColumn","var").json(rdd)
    print("   schema:", d.schema.simpleString())
    print("   ->", [str(r[0]) for r in d.collect()])
except Exception as e:
    print("   ERROR", type(e).__name__, str(e).split("\n")[0][:100])
spark.stop()
