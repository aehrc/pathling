import tempfile, os
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
tmp = tempfile.mkdtemp()

# Decimal-ish values whose lexical form matters.
DOCS = [
    '{"id":"a","v":1.50}',
    '{"id":"b","v":1e2}',
    '{"id":"c","v":1.0e-7}',
    '{"id":"d","v":1234567890123456789012345678901234567890.5}',
    '{"id":"e","v":0.000000001}',
    '{"id":"f","v":100}',
]
# Target schema declares the decimal as STRING (the Parquet on FHIR representation).
schema = StructType([StructField("id", StringType()), StructField("v", StringType())])

def show(label, df):
    try:
        vals = [(r["id"], r["v"]) for r in df.orderBy("id").collect()]
        print(f"  {label:<44} {vals}")
    except Exception as e:
        print(f"  {label:<44} ERROR {type(e).__name__}: {str(e).split(chr(10))[0][:90]}")

print("source lexical forms:", [d.split('"v":')[1].rstrip('}') for d in DOCS])
print()

# 1. file-based JSONL, explicit schema
p = tmp + "/jsonl"; os.makedirs(p)
with open(p + "/part.json", "w") as f:
    f.write("\n".join(DOCS) + "\n")
show("1. read.schema(...).json(path)  [JSONL file]", spark.read.schema(schema).json(p))

# 2. Dataset<String> via RDD  -- the PathlingContext.encode path
rdd = spark.sparkContext.parallelize(DOCS)
show("2. read.schema(...).json(RDD[String])", spark.read.schema(schema).json(rdd))

# 3. Dataset<String> via a DataFrame column -> Scala Dataset[String]
dfs = spark.createDataFrame([(d,) for d in DOCS], "value string")
try:
    jds = dfs._jdf.select(dfs["value"]._jc).as_(spark._jvm.org.apache.spark.sql.Encoders.STRING())
    jdf = spark._jsparkSession.read().schema(schema.json()).json(jds)
    from pyspark.sql import DataFrame
    show("3. read.schema(...).json(Dataset[String])", DataFrame(jdf, spark))
except Exception as e:
    print("  3. Dataset[String] via JVM               ERROR", type(e).__name__, str(e).split("\n")[0][:110])

# 4. from_json over a string column
show("4. from_json(col, schema)",
     dfs.select(F.from_json("value", schema).alias("s")).select("s.id", "s.v"))

# 5. multiline file
pm = tmp + "/multi"; os.makedirs(pm)
with open(pm + "/part.json", "w") as f:
    f.write("[" + ",".join(DOCS) + "]\n")
show("5. multiline JSON file", spark.read.schema(schema).option("multiLine", "true").json(pm))

# 6. what the config says
print()
print("  spark.sql.json.enableExactStringParsing =",
      spark.conf.get("spark.sql.json.enableExactStringParsing", "<unset>"))
spark.stop()
