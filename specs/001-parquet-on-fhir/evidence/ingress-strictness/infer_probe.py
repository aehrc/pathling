"""Shows how Spark's JSON reader infers types: from the token type, or from the content."""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("spark", spark.version)

def show(label, rows, **opts):
    print(f"\n== {label} {opts or ''}")
    try:
        df = spark.read.options(**opts).json(spark.sparkContext.parallelize(rows))
        rs = df.collect()
    except Exception as e:
        print("   ERROR", str(e).splitlines()[0][:200])
        return
    for f in df.schema.fields:
        print(f"   {f.name}: {f.dataType.simpleString()}")
    for r in rs:
        print("   ", json.dumps(r.asDict(recursive=True), default=str))

one = ['{"s":"5","d":"2020-01-01","ts":"2020-01-01T10:00:00Z","b":"true","i":5,"big":99999999999999999999,"f":1.50,"e":1.0e2,"n":null}']
show("single row, string contents look like numbers/dates", one)
show("same, prefersDecimal", one, prefersDecimal="true")
show("prefersDecimal without the exponent value", [one[0].replace(',"e":1.0e2','')], prefersDecimal="true")
show("same, inferTimestamp", one, inferTimestamp="true")
show("same, primitivesAsString", one, primitivesAsString="true")
show("int then float in one column", ['{"v":5}', '{"v":1.50}'])
show("number then string in one column", ['{"v":5}', '{"v":"x"}'])
show("number then string, number given 1.50", ['{"v":1.50}', '{"v":"x"}'])
show("bool then string", ['{"v":true}', '{"v":"x"}'])
show("scalar then array", ['{"v":"a"}', '{"v":["b","c"]}'])
show("scalar then object", ['{"v":"a"}', '{"v":{"x":1}}'])
show("all null", ['{"v":null}'])
