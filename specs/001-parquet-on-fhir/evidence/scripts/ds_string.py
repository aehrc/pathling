from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled","false")
         .config("spark.sql.shuffle.partitions","1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
DOCS = ['{"id":"a","v":1.50}','{"id":"b","v":1e2}',
        '{"id":"d","v":1234567890123456789012345678901234567890.5}']
schema = StructType([StructField("id",StringType()),StructField("v",StringType())])
dfs = spark.createDataFrame([(d,) for d in DOCS], "value string")
enc = spark._jvm.org.apache.spark.sql.Encoders.STRING()
jds = dfs._jdf.as_(enc)                      # a genuine Dataset[String]
print("jds class:", jds.getClass().getName())
jdf = spark._jsparkSession.read().schema(schema.json()).json(jds)
out = DataFrame(jdf, spark)
print("Dataset[String] overload:", [(r["id"], r["v"]) for r in out.orderBy("id").collect()])
spark.stop()
