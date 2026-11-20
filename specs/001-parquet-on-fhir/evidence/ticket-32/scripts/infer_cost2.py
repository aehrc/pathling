import time, sys
from pyspark.sql import functions as F
from pathling import PathlingContext
pc = PathlingContext.create(enable_terminology=False); spark = pc.spark; spark.sparkContext.setLogLevel("ERROR")
raw = spark.read.text(sys.argv[1]).cache(); raw.count()
jvm = spark._jvm
def ds(df): return getattr(df._jdf, "as")(jvm.org.apache.spark.sql.Encoders.STRING())
stripped = raw.select(F.regexp_replace("value", r'value="(?!(?:-?[0-9][0-9.eE+-]*|true|false)")[^"]*"', 'value=""').alias("value"))
def t(label, fn):
    for i in range(3):
        s=time.perf_counter(); r=fn()
    print(label, round((time.perf_counter()-s)*1000), flush=True); return r
rd = lambda inf: spark._jsparkSession.read().option("rowTag","Observation").option("inferSchema",inf).option("ignoreSurroundingSpaces","false").option("mode","FAILFAST")
t("noop strip", lambda: stripped.write.format("noop").mode("overwrite").save())
s1 = t("infer true stripped", lambda: rd("true").xml(ds(stripped)).schema())
s0 = t("infer false raw", lambda: rd("false").xml(ds(raw)).schema())
print(s1.simpleString()[:1500])
