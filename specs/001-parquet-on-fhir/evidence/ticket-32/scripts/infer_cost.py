import time, sys
from pathling import PathlingContext
pc = PathlingContext.create(enable_terminology=False); spark = pc.spark; spark.sparkContext.setLogLevel("ERROR")
raw = spark.read.text(sys.argv[1]).cache(); raw.count()
jvm = spark._jvm
docs = getattr(raw._jdf, "as")(jvm.org.apache.spark.sql.Encoders.STRING())
def t(label, fn):
    for i in range(3):
        s=time.perf_counter(); fn(); 
    print(label, round((time.perf_counter()-s)*1000), flush=True)
for inf in ["false", "true"]:
    t("infer "+inf, lambda: spark._jsparkSession.read().option("rowTag","Observation").option("inferSchema",inf).option("ignoreSurroundingSpaces","false").option("mode","FAILFAST").xml(docs).schema())
t("infer true preferDate=false", lambda: spark._jsparkSession.read().option("rowTag","Observation").option("inferSchema","true").option("preferDate","false").option("timestampFormat","'x'").option("timestampNTZFormat","'x'").option("ignoreSurroundingSpaces","false").option("mode","FAILFAST").xml(docs).schema())
t("pc.encode fit", lambda: pc.encode(raw, "Observation", input_type="application/fhir+xml"))
