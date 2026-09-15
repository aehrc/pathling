import tempfile
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
tmp = tempfile.mkdtemp(); p = tmp + "/f"

narrow = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("given", ArrayType(StringType()))])))])
wide = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("family", StringType()),
        StructField("given", ArrayType(StringType()))])))])

spark.createDataFrame([Row(id="n1", name=[Row(given=["Kim"]), Row(given=["Lee"])])],
                      schema=narrow).write.mode("overwrite").parquet(p)

def rd():
    return spark.read.schema(wide).parquet(p)

patterns = {
    "array_size(name)":              lambda d: d.select(F.expr("array_size(name)").alias("v")),
    "size(name)":                    lambda d: d.select(F.size("name").alias("v")),
    "name.family (no explode)":      lambda d: d.select(F.col("name.family").alias("v")),
    "name.given (present leaf)":     lambda d: d.select(F.col("name.given").alias("v")),
    "explode(name) then n.family":   lambda d: d.select(F.explode("name").alias("n")).select(F.col("n.family").alias("v")),
    "explode(name) whole struct":    lambda d: d.select(F.explode("name").alias("v")),
    "explode(name.family)":          lambda d: d.select(F.explode("name.family").alias("v")),
    "filter name.family is null":    lambda d: d.select(F.col("id"), F.col("name.family").alias("v")),
    "exists(name, x->x.family is not null)":
                                     lambda d: d.select(F.expr("exists(name, x -> x.family is not null)").alias("v")),
    "transform(name, x->x.family)":  lambda d: d.select(F.expr("transform(name, x -> x.family)").alias("v")),
}

for pruning in (True, False):
    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", str(pruning).lower())
    print(f"\n=== nestedSchemaPruning={pruning} ===")
    for label, fn in patterns.items():
        try:
            rows = fn(rd()).collect()
            vals = [r["v"] for r in rows]
            print(f"  {label:<40} rows={len(rows)} {vals}")
        except Exception as e:
            print(f"  {label:<40} ERROR {type(e).__name__}: {str(e).split(chr(10))[0][:80]}")

print("\nGround truth: one row, name has 2 elements, family is absent (null) for both.")
spark.stop()
