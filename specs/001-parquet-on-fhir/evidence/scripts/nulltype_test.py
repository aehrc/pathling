import tempfile, os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

tmp = tempfile.mkdtemp()

def check(label, fn):
    try:
        r = fn()
        print(f"OK    | {label} | {r}")
    except Exception as e:
        msg = str(e).split("\n")[0][:180]
        print(f"FAIL  | {label} | {type(e).__name__}: {msg}")

base = spark.range(3).toDF("id")

# 1. top-level NullType column
df_null = base.withColumn("absent", F.lit(None))
print("schema 1:", df_null.schema.simpleString())
check("NullType -> parquet", lambda: (df_null.write.mode("overwrite").parquet(tmp + "/p1"), "written")[1])
check("NullType -> json", lambda: (df_null.write.mode("overwrite").json(tmp + "/j1"), "written")[1])
check("NullType -> delta-less orc", lambda: (df_null.write.mode("overwrite").orc(tmp + "/o1"), "written")[1])

# 2. NullType nested inside a struct
df_nested = base.withColumn("s", F.struct(F.lit(None).alias("absent"), F.col("id").cast("string").alias("code")))
print("schema 2:", df_nested.schema.simpleString())
check("nested NullType -> parquet", lambda: (df_nested.write.mode("overwrite").parquet(tmp + "/p2"), "written")[1])

# 3. empty struct
try:
    df_empty = base.withColumn("s", F.struct())
    print("schema 3:", df_empty.schema.simpleString())
    check("empty struct -> parquet", lambda: (df_empty.write.mode("overwrite").parquet(tmp + "/p3"), "written")[1])
    check("empty struct -> json", lambda: (df_empty.write.mode("overwrite").json(tmp + "/j3"), "written")[1])
except Exception as e:
    print("FAIL  | build empty struct |", type(e).__name__, str(e).split("\n")[0][:180])

# 3b. empty struct via explicit schema read
check("read with empty-struct schema",
      lambda: spark.read.schema(StructType([StructField("a", StructType([]))])).json(
          spark.sparkContext.parallelize(['{"a":{"x":1}}']).toDF("value").rdd.map(lambda r: r[0])).schema.simpleString())

# 4. struct<id:string> control
df_minimal = base.withColumn("s", F.struct(F.lit(None).cast("string").alias("id")))
print("schema 4:", df_minimal.schema.simpleString())
check("struct<id:string> -> parquet", lambda: (df_minimal.write.mode("overwrite").parquet(tmp + "/p4"), "written")[1])

# 5. to_json over NullType
check("to_json(NullType)", lambda: df_null.select(F.to_json(F.struct("id", "absent")).alias("j")).first()["j"])
check("to_json(struct<id:string> all null)", lambda: df_minimal.select(F.to_json(F.struct("id", "s")).alias("j")).first()["j"])

# 6. NullType coercion in a union / coalesce against a struct
df_s = base.withColumn("s", F.struct(F.lit("x").alias("id"), F.lit("y").alias("family")))
check("coalesce(NullType, struct<id,family>)",
      lambda: df_s.select(F.coalesce(F.lit(None), F.col("s")).alias("c")).schema.simpleString())
check("coalesce(struct<id>, struct<id,family>)",
      lambda: df_s.select(F.coalesce(F.struct(F.lit(None).cast("string").alias("id")), F.col("s")).alias("c")).schema.simpleString())
check("array_union(array<null>, array<struct>)",
      lambda: df_s.select(F.array_union(F.array(F.lit(None)), F.array(F.col("s"))).alias("c")).schema.simpleString())

# 7. reading back a parquet written with NullType
check("read back p1", lambda: spark.read.parquet(tmp + "/p1").schema.simpleString())

spark.stop()
