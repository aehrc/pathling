"""What can the fallback type of an absent element be?

The candidates for an absent COMPLEX element:
  (a) void / NullType
  (b) struct<>          (empty struct)
  (c) struct<id:string> (minimal struct)

The requirement is FR-027: combining an absent element with a populated one
must succeed. That is a coalesce/if across the fallback type and the real type.

Plus the repeating case: the engine wraps arrays in transform(), so the
fallback for an absent repeating element must survive transform().
"""
import os
import tempfile

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, NullType

spark = SparkSession.builder.master("local[2]").appName("fallback").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.sql("""
  select named_struct('id','1','family','Smith') as real,
         array(named_struct('id','1','family','Smith')) as reals
""")

EMPTY = StructType([])
MINIMAL = StructType([StructField("id", StringType())])


def case(label, fn):
    try:
        d = fn()
        print(f"OK   {label}\n       -> {d.schema.simpleString()}  rows={d.collect()}")
    except Exception as e:
        msg = str(e).split("\n")[0][:150]
        print(f"FAIL {label}\n       -> {msg}")


print("=== 1. can the fallback literal even be constructed? ===")
case("void literal", lambda: df.select(F.lit(None).alias("x")))
case("empty-struct literal", lambda: df.select(F.lit(None).cast(EMPTY).alias("x")))
case("minimal-struct literal", lambda: df.select(F.lit(None).cast(MINIMAL).alias("x")))

print("\n=== 2. FR-027: coalesce(absent, populated) for a singular complex element ===")
case("coalesce(void, struct)",
     lambda: df.select(F.coalesce(F.lit(None), F.col("real")).alias("x")))
case("coalesce(struct<>, struct)",
     lambda: df.select(F.coalesce(F.lit(None).cast(EMPTY), F.col("real")).alias("x")))
case("coalesce(struct<id>, struct)",
     lambda: df.select(F.coalesce(F.lit(None).cast(MINIMAL), F.col("real")).alias("x")))

print("\n=== 3. traversal INTO the fallback (the composition step) ===")
case("void . family", lambda: df.select(F.lit(None).getField("family").alias("x")))
case("struct<> . family", lambda: df.select(F.lit(None).cast(EMPTY).getField("family").alias("x")))

print("\n=== 4. absent REPEATING element: must survive transform() ===")
case("transform(void)",
     lambda: df.select(F.transform(F.lit(None), lambda x: x).alias("x")))
case("transform(array<void>)",
     lambda: df.selectExpr("transform(cast(null as array<void>), x -> x) as x"))
case("transform(array<struct<>>)",
     lambda: df.select(
         F.transform(F.lit(None).cast(ArrayType(EMPTY)), lambda x: x).alias("x")))
case("transform(array<void>) with a real body",
     lambda: df.selectExpr(
         "transform(cast(null as array<void>), x -> cast(null as string)) as x"))

print("\n=== 5. FR-027 for a repeating element ===")
case("coalesce(array<void>, array<struct>)",
     lambda: df.selectExpr("coalesce(cast(null as array<void>), reals) as x"))
case("coalesce(array<struct<>>, array<struct>)",
     lambda: df.select(
         F.coalesce(F.lit(None).cast(ArrayType(EMPTY)), F.col("reals")).alias("x")))

print("\n=== 6. does an empty struct survive a write? (storage, not expression) ===")
case("write struct<> to parquet",
     lambda: (df.select(F.lit(None).cast(EMPTY).alias("x"))
              .write.mode("overwrite")
              .parquet(os.path.join(tempfile.gettempdir(), "es"))) or df.limit(0))

spark.stop()
