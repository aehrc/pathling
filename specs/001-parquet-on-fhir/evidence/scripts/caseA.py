import tempfile
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
tmp = tempfile.mkdtemp(); p = tmp + "/only-file"

# ---- What gets written: ONE file, whose `name` struct has NO `family` leaf.
narrow = StructType([
    StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("given", ArrayType(StringType()))])))])

df = spark.createDataFrame(
    [Row(id="n1", name=[Row(given=["Kim"]), Row(given=["Lee"])])], schema=narrow)
df.write.mode("overwrite").parquet(p)

print("=" * 78)
print("DATA WRITTEN (one row, one file)")
df.show(truncate=False)

print("=" * 78)
print("SCHEMA OF THE FILE ON DISK")
print(spark.read.parquet(p).schema.treeString())

print("=" * 78)
print("WIDER READ SCHEMA SUPPLIED TO spark.read.schema(...)  [adds name.family]")
wide = StructType([
    StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("family", StringType()),
        StructField("given", ArrayType(StringType()))])))])
print(wide.treeString())

print("=" * 78)
print("QUERY:  read.schema(wide).parquet(p)")
print("          .select(id, explode(name) AS n)")
print("          .select(id, n.family)")
print()
print("EXPECTED: 2 rows  [(n1, null), (n1, null)]   -- the patient has two names")
print()

for pruning in (True, False):
    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", str(pruning).lower())
    q = (spark.read.schema(wide).parquet(p)
         .select(F.col("id"), F.explode(F.col("name")).alias("n"))
         .select("id", "n.family"))
    got = q.collect()
    print(f"  nestedSchemaPruning={str(pruning):<5} -> {len(got)} rows: {[(r['id'], r['family']) for r in got]}")

print()
print("Same query but also selecting the sibling leaf `given`, which IS in the file:")
spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
q2 = (spark.read.schema(wide).parquet(p)
      .select(F.col("id"), F.explode(F.col("name")).alias("n"))
      .select("id", "n.family", "n.given"))
got2 = q2.collect()
print(f"  nestedSchemaPruning=true  -> {len(got2)} rows: {[(r['id'], r['family'], r['given']) for r in got2]}")

print()
print("=" * 78)
print("PHYSICAL PARQUET SCHEMA (what leaves actually exist in the file)")
print("=" * 78)
try:
    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()
    jpath = sc._jvm.org.apache.hadoop.fs.Path(p)
    fs = jpath.getFileSystem(hconf)
    for st in fs.listStatus(jpath):
        n = st.getPath().getName()
        if n.endswith(".parquet"):
            rdr = sc._jvm.org.apache.parquet.hadoop.ParquetFileReader.open(hconf, st.getPath())
            print(rdr.getFooter().getFileMetaData().getSchema().toString())
            rdr.close()
except Exception as e:
    print("(could not read footer:", type(e).__name__, str(e)[:120], ")")

spark.stop()
