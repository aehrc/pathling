import tempfile, glob, os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

jars = ",".join(sorted(glob.glob(os.path.expanduser("~/.ivy2*/jars/io.delta_delta-*.jar"))))
print("jars:", jars)

spark = (SparkSession.builder.master("local[1]")
         .config("spark.jars", jars)
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
tmp = tempfile.mkdtemp(); p = tmp + "/t"

narrow = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("given", ArrayType(StringType()))])))])
wide = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("family", StringType()),
        StructField("given", ArrayType(StringType()))])))])

# Commit 1: narrow schema, no `family` leaf anywhere in this file.
spark.createDataFrame([Row(id="n1", name=[Row(given=["Kim"]), Row(given=["Lee"])])],
                      schema=narrow).write.format("delta").mode("overwrite").save(p)
# Commit 2: schema evolution adds `family`.
spark.createDataFrame([Row(id="w1", name=[Row(family="Smith", given=["Jo"])])],
                      schema=wide).write.format("delta").mode("append") \
     .option("mergeSchema", "true").save(p)

df = spark.read.format("delta").load(p)
print("table schema from the log:", df.schema.simpleString())

ex = df.select(F.col("id"), F.explode(F.col("name")).alias("n"))
for label, sel in [("select ONLY n.family", ex.select("id", "n.family")),
                   ("select n.family + n.given", ex.select("id", "n.family", "n.given"))]:
    got = sel.collect()
    print(f"  {label:<28} -> {len(got)} rows: {[r['id'] for r in got]}")

spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "false")
got = spark.read.format("delta").load(p).select(F.col("id"), F.explode(F.col("name")).alias("n")).select("id","n.family").collect()
print(f"  pruning OFF, only n.family   -> {len(got)} rows: {[r['id'] for r in got]}")
print("EXPECTED in all cases: 3 rows (n1 twice, w1 once)")
spark.stop()
