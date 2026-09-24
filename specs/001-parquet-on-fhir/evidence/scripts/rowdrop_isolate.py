import tempfile
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
tmp = tempfile.mkdtemp()

wide = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("family", StringType()),
        StructField("given", ArrayType(StringType()))])))])
narrow = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("given", ArrayType(StringType()))])))])

dfn = spark.createDataFrame([Row(id="n1", name=[Row(given=["Kim"]), Row(given=["Lee"])])], schema=narrow)
dfw = spark.createDataFrame([Row(id="w1", name=[Row(family="Smith", given=["Jo"])])], schema=wide)

def probe(label, df):
    ex = df.select(F.col("id"), F.explode(F.col("name")).alias("n"))
    only = ex.select("id", "n.family").collect()
    both = ex.select("id", "n.family", "n.given").collect()
    print(f"  {label:<52} only-missing-leaf={len(only)} rows {[r['id'] for r in only]} | +sibling={len(both)} rows")

# A. ONE narrow file only, read with an EXPLICIT wider schema
p = tmp + "/a"; dfn.write.mode("overwrite").parquet(p)
probe("A. single narrow file, explicit wide read schema", spark.read.schema(wide).parquet(p))

# B. ONE narrow file, read natively: the sparse schema itself, nothing missing
b = spark.read.parquet(p).select(F.col("id"), F.explode(F.col("name")).alias("n")).select("id", "n.given").collect()
print(f"  B. single narrow file, native sparse schema (control)  only-leaf={len(b)} rows {[r['id'] for r in b]}")

# C. TWO files, both narrow, uniform sparse schema (no divergence)
p2 = tmp + "/c"; dfn.write.mode("overwrite").parquet(p2); dfn.write.mode("append").parquet(p2)
d = spark.read.option("mergeSchema","true").parquet(p2)
c = d.select(F.col("id"), F.explode(F.col("name")).alias("n")).select("id", "n.given").collect()
print(f"  C. two uniform narrow files, merged, no divergence     only-leaf={len(c)} rows {[r['id'] for r in c]}")

# D. TWO divergent files, merged (the earlier repro)
p3 = tmp + "/d"; dfw.write.mode("overwrite").parquet(p3); dfn.write.mode("append").parquet(p3)
probe("D. two divergent files, mergeSchema", spark.read.option("mergeSchema","true").parquet(p3))

# E. TWO divergent files, explicit wide schema instead of mergeSchema
probe("E. two divergent files, explicit wide read schema", spark.read.schema(wide).parquet(p3))

# F. single WIDE file read with wide schema (no missing leaf anywhere)
p4 = tmp + "/f"; dfw.write.mode("overwrite").parquet(p4)
probe("F. single wide file, nothing missing (control)", spark.read.parquet(p4))

spark.stop()
