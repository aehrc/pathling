import tempfile, os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
tmp = tempfile.mkdtemp(); path = tmp + "/t"

wide = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("family", StringType()),
        StructField("given", ArrayType(StringType()))])))])
# Narrow file: `family` never written, but the anchor leaf `id` IS in the schema, all null.
narrow = StructType([StructField("id", StringType()),
    StructField("name", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("given", ArrayType(StringType()))])))])

spark.createDataFrame([Row(id="wide-1", name=[
    Row(id=None, family="Smith", given=["Jo"]), Row(id=None, family="Jones", given=["Al"])])],
    schema=wide).write.mode("overwrite").parquet(path)
spark.createDataFrame([Row(id="narrow-1", name=[
    Row(id=None, given=["Kim"]), Row(id=None, given=["Lee"])])],
    schema=narrow).write.mode("append").parquet(path)

df = spark.read.option("mergeSchema", "true").parquet(path)
ex = df.select(F.col("id"), F.explode(F.col("name")).alias("n"))

def rows(label, sel):
    got = sel.collect()
    ids = [r["id"] for r in got]
    print(f"  {label:<46} rows={len(got)} ids={ids}")

print("anchor leaf `name.id` present in BOTH files, all null in the narrow one:")
rows("select only the missing leaf (family)", ex.select("id", "n.family"))
rows("select missing leaf + anchor (family, id)", ex.select("id", "n.family", F.col("n.id").alias("anchor")))
rows("select only the anchor (id)", ex.select("id", F.col("n.id").alias("anchor")))

# Does an all-null anchor actually get written as a leaf?
import glob, subprocess
f = sorted(glob.glob(path + "/*.parquet"))
print("\nparquet files:", len(f))
print("\nschema of merged read:", df.schema.simpleString())
spark.stop()
