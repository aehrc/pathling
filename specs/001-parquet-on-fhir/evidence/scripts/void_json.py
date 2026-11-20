import tempfile, os, glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
tmp = tempfile.mkdtemp()

def show(label, fn):
    try:
        print(f"OK    | {label} | {fn()}")
    except Exception as e:
        print(f"FAIL  | {label} | {type(e).__name__}: {str(e).split(chr(10))[0][:150]}")

def read_files(path):
    out = []
    for f in sorted(glob.glob(path + "/*.json")):
        with open(f) as fh:
            out.extend(l.rstrip() for l in fh if l.strip())
    return out

base = spark.range(2).toDF("id")

# A. top-level void, write.json
df = base.withColumn("absent", F.lit(None)).withColumn("present", F.lit("v"))
print("schema A:", df.schema.simpleString())
df.write.mode("overwrite").json(tmp + "/a")
show("A write.json (default)", lambda: read_files(tmp + "/a"))

df.write.mode("overwrite").option("ignoreNullFields", "false").json(tmp + "/a2")
show("A write.json ignoreNullFields=false", lambda: read_files(tmp + "/a2"))

# B. to_json
show("B to_json default", lambda: df.select(F.to_json(F.struct("id","absent","present")).alias("j")).first()["j"])
show("B to_json ignoreNullFields=false",
     lambda: df.select(F.to_json(F.struct("id","absent","present"), {"ignoreNullFields":"false"}).alias("j")).first()["j"])

# C. void nested inside a struct
dfn = base.withColumn("s", F.struct(F.lit(None).alias("absent"), F.lit("c").alias("code")))
print("schema C:", dfn.schema.simpleString())
show("C to_json default", lambda: dfn.select(F.to_json(F.struct("id","s")).alias("j")).first()["j"])
show("C to_json ignoreNullFields=false",
     lambda: dfn.select(F.to_json(F.struct("id","s"), {"ignoreNullFields":"false"}).alias("j")).first()["j"])
dfn.write.mode("overwrite").json(tmp + "/c")
show("C write.json default", lambda: read_files(tmp + "/c"))

# D. struct containing ONLY a void field
dfo = base.withColumn("s", F.struct(F.lit(None).alias("absent")))
print("schema D:", dfo.schema.simpleString())
show("D to_json default", lambda: dfo.select(F.to_json(F.struct("id","s")).alias("j")).first()["j"])
show("D to_json ignoreNullFields=false",
     lambda: dfo.select(F.to_json(F.struct("id","s"), {"ignoreNullFields":"false"}).alias("j")).first()["j"])
show("D write.json", lambda: (dfo.write.mode("overwrite").json(tmp + "/d"), read_files(tmp + "/d"))[1])

# E. array<void>
dfa = base.withColumn("a", F.array(F.lit(None)))
print("schema E:", dfa.schema.simpleString())
show("E to_json default", lambda: dfa.select(F.to_json(F.struct("id","a")).alias("j")).first()["j"])
show("E write.json", lambda: (dfa.write.mode("overwrite").json(tmp + "/e"), read_files(tmp + "/e"))[1])

# F. all-null struct<id:string> for comparison
dfm = base.withColumn("s", F.struct(F.lit(None).cast("string").alias("id")))
show("F to_json struct<id> all-null", lambda: dfm.select(F.to_json(F.struct("id","s")).alias("j")).first()["j"])
show("F write.json struct<id> all-null", lambda: (dfm.write.mode("overwrite").json(tmp + "/f"), read_files(tmp + "/f"))[1])

# G. read back the void-written json with inference
show("G read back A inferred", lambda: spark.read.json(tmp + "/a").schema.simpleString())

spark.stop()
