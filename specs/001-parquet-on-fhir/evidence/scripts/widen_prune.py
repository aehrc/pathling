"""Does nested schema pruning survive a widening projection?

Case 1: baseline - select a leaf directly from a stored array<struct<...>>.
Case 2: rebuild the array with transform(arr, x -> struct(...)) adding a null
        field, then select the same leaf.
Case 3: same for a singular (non-array) struct rebuild.

We report the ReadSchema from the physical plan, which is what Parquet
actually reads.
"""
import re
import os
import tempfile

from pyspark.sql import SparkSession

OUT = os.path.join(tempfile.gettempdir(), "widen_data")

spark = (
    SparkSession.builder.master("local[2]")
    .appName("widen_prune")
    .config("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

spark.sql("""
  select 'p1' as id,
         array(named_struct('family','Smith','given',array('Jo'),'big','XXXX'),
               named_struct('family','Doe','given',array('Al'),'big','YYYY')) as name,
         named_struct('city','Perth','state','WA','big','ZZZZ') as addr
""").write.mode("overwrite").parquet(OUT)

df = spark.read.parquet(OUT)
print("stored schema:", df.schema.simpleString())


def read_schema(plan: str) -> str:
    m = re.findall(r"ReadSchema:\s*(struct<.*?>)\s*$", plan, re.M)
    return m[-1] if m else "<not found>"


def show(label, d):
    plan = d._jdf.queryExecution().executedPlan().toString()
    print(f"\n--- {label}\n  ReadSchema: {read_schema(plan)}")


# Case 1: direct leaf selection, no rebuild.
show("1 baseline: transform(name, x -> x.family)",
     df.selectExpr("transform(name, x -> x.family) as f"))

# Case 2: widen the array elements by rebuilding the struct, then take a leaf.
widen_arr = (
    "transform(name, x -> named_struct("
    "'family', x.family, 'given', x.given, 'big', x.big, "
    "'extra', cast(null as string))) as name"
)
show("2 widened array, then transform(...x.family)",
     df.selectExpr(widen_arr).selectExpr("transform(name, x -> x.family) as f"))

# Case 2b: widen and select through inline (what the engine actually does).
show("2b widened array, inline, then .family",
     df.selectExpr(widen_arr).selectExpr("inline(name)").select("family"))

# Case 3: singular struct rebuild.
widen_struct = (
    "named_struct('city', addr.city, 'state', addr.state, 'big', addr.big, "
    "'extra', cast(null as string)) as addr"
)
show("3 baseline: addr.city", df.select("addr.city"))
show("3b widened struct, then .city",
     df.selectExpr(widen_struct).select("addr.city"))

spark.stop()
