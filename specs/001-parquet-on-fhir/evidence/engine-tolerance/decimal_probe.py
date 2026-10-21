import json, sys, os, re
from pyspark.sql import SparkSession, functions as F
jar = os.path.expanduser("~/.m2/repository/au/csiro/pathling/library-runtime/10.0.0-SNAPSHOT/library-runtime-10.0.0-SNAPSHOT.jar")
spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled","false").config("spark.jars", jar).getOrCreate()
spark.sparkContext.setLogLevel("OFF")
sys.path.insert(0, sys.argv[1])
from pathling import PathlingContext
pc = PathlingContext.create(spark, enable_terminology=False)
def short(e):
    hits = [l.strip() for l in str(e).splitlines() if re.search(r"(Exception:|Error:)", l) and "collectToPython" not in l]
    return (hits[0] if hits else str(e).splitlines()[0])[:220]
res = {"resourceType":"Observation","id":"o1","status":"final","code":{"text":"x"},
       "valueQuantity":{"value":1.50,"unit":"mg","system":"http://unitsofmeasure.org","code":"mg"}}
base = pc.encode(spark.createDataFrame([(json.dumps(res),)],["v"]), "Observation")
vq = base.schema["valueQuantity"].dataType
print("valueQuantity type:", vq.simpleString())
print("stored:", base.select("valueQuantity").collect()[0][0])
fields = [f.name for f in vq.fields]
for bad in ["1.50", "1.2.3", "abc", "", " 1.5", "1e400", "12345678901234567890123456789.5", "0.0000001"]:
    df = base.withColumn("valueQuantity", F.col("valueQuantity").withField("value", F.lit(bad)))
    ds = pc.read.datasets({"Observation": df})
    print(f"=== stored value text {bad!r}")
    for label, path in [("select", "value.ofType(Quantity).value"),
                        ("exists", "value.ofType(Quantity).value.exists()"),
                        ("> 1", "value.ofType(Quantity).value > 1"),
                        ("= 1.5", "value.ofType(Quantity).value = 1.5"),
                        ("+ 1", "value.ofType(Quantity).value + 1"),
                        ("qty > 1 'mg'", "value.ofType(Quantity) > 1 'mg'"),
                        ("toString", "value.ofType(Quantity).value.toString()")]:
        try:
            r = ds.view("Observation", select=[{"column":[{"path":path,"name":"v"}]}]).collect()[0][0]
            print(f"  {label:14s} -> {r!r}")
        except Exception as e:
            print(f"  {label:14s} -> ERROR {short(e)}")
