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

def rd(): return spark.read.schema(wide).parquet(p)

# Pathling's actual shape for `forEach: name { column: family }`:
#   transform(name, x -> struct(x.family as fam))   <- lambda takes the WHOLE element
#   then flatten (no-op when the inner result is not nested)
#   then inline(...) once, at the top of the projection
PATHLING_FOREACH = (
    "inline(transform(name, x -> struct(x.family as fam)))")

# Same, but with the inner component itself producing an array (the nested-forEach case),
# so flatten is genuinely exercised.
PATHLING_NESTED = (
    "inline(flatten(transform(name, x -> transform(x.given, g -> struct(x.family as fam, g as giv)))))")

# For contrast: the naive explode shape Pathling does NOT use.
NAIVE_EXPLODE = None

cases = {
    "Pathling forEach shape (transform+inline)": lambda d: d.selectExpr("id", PATHLING_FOREACH),
    "Pathling nested forEach (transform+flatten+inline)": lambda d: d.selectExpr("id", PATHLING_NESTED),
    "naive explode(name) then n.family": lambda d: d.select(F.col("id"), F.explode("name").alias("n")).select("id", "n.family"),
}

for pruning in (True, False):
    spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", str(pruning).lower())
    print(f"\n=== nestedSchemaPruning={pruning} ===")
    for label, fn in cases.items():
        try:
            rows = fn(rd()).collect()
            print(f"  {label:<52} rows={len(rows)}  {[tuple(r) for r in rows]}")
        except Exception as e:
            print(f"  {label:<52} ERROR {type(e).__name__}: {str(e).split(chr(10))[0][:90]}")

print("\nGround truth: name has 2 elements, family absent for both.")
print("  forEach shape should give 2 rows; nested should give 2 rows (one given each).")
spark.stop()
