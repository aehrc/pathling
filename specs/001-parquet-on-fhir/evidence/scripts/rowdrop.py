import tempfile, shutil, os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
tmp = tempfile.mkdtemp()
path = tmp + "/t"

# Wide schema: name is array<struct<family, given>>
wide = StructType([
    StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("family", StringType()),
        StructField("given", ArrayType(StringType())),
    ]))),
])
# Narrow schema: the SAME element, but the `family` leaf was never written.
narrow = StructType([
    StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("given", ArrayType(StringType())),
    ]))),
])

dfw = spark.createDataFrame(
    [Row(id="wide-1", name=[Row(family="Smith", given=["Jo"]), Row(family="Jones", given=["Al"])])],
    schema=wide)
dfn = spark.createDataFrame(
    [Row(id="narrow-1", name=[Row(given=["Kim"]), Row(given=["Lee"])])],
    schema=narrow)

dfw.write.mode("overwrite").parquet(path)
dfn.write.mode("append").parquet(path)

print("files:", sorted(os.path.basename(f) for f in os.listdir(path) if f.endswith(".parquet")))

def run(label, pruning):
    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", str(pruning).lower())
    df = spark.read.option("mergeSchema", "true").parquet(path)
    # 1. explode selecting ONLY the leaf missing from the narrow file
    only = df.select(F.col("id"), F.explode(F.col("name")).alias("n")).select("id", "n.family")
    rows_only = [(r["id"], r["family"]) for r in only.collect()]
    # 2. same, but also selecting a sibling leaf present in both files
    sib = df.select(F.col("id"), F.explode(F.col("name")).alias("n")).select("id", "n.family", "n.given")
    rows_sib = [(r["id"], r["family"], r["given"]) for r in sib.collect()]
    # 3. the array itself, unexploded
    arr = df.select("id", F.size("name").alias("n_names")).collect()
    print(f"\n--- {label} (nestedSchemaPruning={pruning}) ---")
    print("  explode, ONLY missing leaf   :", rows_only)
    print("  explode, missing + sibling   :", rows_sib)
    print("  size(name) per row           :", [(r['id'], r['n_names']) for r in arr])

run("merged parquet", True)
run("merged parquet", False)

# Control: same test on Delta-style single schema (no missing leaf anywhere)
print("\n=== control: no missing leaf ===")
spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
ctl = tmp + "/c"
dfw.write.mode("overwrite").parquet(ctl)
dfw.write.mode("append").parquet(ctl)
c = spark.read.parquet(ctl).select(F.col("id"), F.explode(F.col("name")).alias("n")).select("id", "n.family")
print("  explode, only leaf:", [(r["id"], r["family"]) for r in c.collect()])

spark.stop()
