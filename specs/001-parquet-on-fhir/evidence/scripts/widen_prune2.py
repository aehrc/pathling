"""Does nested pruning apply to arrays at all, and does transform block it?"""
import re
import os
import tempfile

from pyspark.sql import SparkSession

OUT = os.path.join(tempfile.gettempdir(), "widen_data")

spark = (
    SparkSession.builder.master("local[2]").appName("widen_prune2").getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet(OUT)


def read_schema(plan):
    m = re.findall(r"ReadSchema:\s*(struct<.*?>)\s*$", plan, re.M)
    return m[-1] if m else "<not found>"


def show(label, d):
    print(f"--- {label}\n  {read_schema(d._jdf.queryExecution().executedPlan().toString())}")


show("a. df.select('name.family')  [implicit extraction]", df.select("name.family"))
show("b. explode(name).family", df.selectExpr("explode(name) as n").select("n.family"))
show("c. inline(name) -> family", df.selectExpr("inline(name)").select("family"))
show("d. transform(name, x -> x.family)", df.selectExpr("transform(name, x -> x.family) as f"))
show("e. transform then flatten/inline",
     df.selectExpr("transform(name, x -> x.family) as f").selectExpr("explode(f) as g"))
show("f. name.family after widening rebuild",
     df.selectExpr(
         "transform(name, x -> named_struct('family', x.family, 'given', x.given,"
         " 'big', x.big, 'extra', cast(null as string))) as name"
     ).select("name.family"))

spark.stop()
