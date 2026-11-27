"""Can Spark parse FHIR XML into VARIANT, via from_xml with a variant schema or the reader's singleVariantColumn?"""
import os, tempfile
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
doc = ('<Patient xmlns="http://hl7.org/fhir"><id value="p1"/><active value="true"/>'
       '<name><family value="X"/><given value="Ann"/></name><birthDate value="1980-02-03"/>'
       '<extension url="http://e/x"><valueDecimal value="1.50"/></extension></Patient>')
spark.createDataFrame([(doc,)], "x string").createOrReplaceTempView("t")
for label, sql in [
    ("from_xml(x, 'variant')", "SELECT CAST(from_xml(x, 'variant') AS STRING) AS v FROM t"),
    ("from_xml(x, 'v variant')", "SELECT CAST(from_xml(x, 'v variant').v AS STRING) AS v FROM t"),
    ("schema_of_variant", "SELECT schema_of_variant(from_xml(x, 'variant')) AS v FROM t"),
]:
    try:
        print(f"== {label}\n   {spark.sql(sql).collect()[0][0]}")
    except Exception as e:
        print(f"== {label}\n   ERROR {str(e).splitlines()[0][:220]}")
d = tempfile.mkdtemp(); p = os.path.join(d, "p.xml")
open(p, "w").write(doc)
try:
    df = spark.read.format("xml").option("rowTag", "Patient").option("singleVariantColumn", "v").load(p)
    print("== reader singleVariantColumn\n  ", df.schema.simpleString(), "\n  ", df.selectExpr("CAST(v AS STRING)").collect()[0][0])
except Exception as e:
    print("== reader singleVariantColumn\n   ERROR", str(e).splitlines()[0][:220])
print("== from_xml with per-field variant schema")
r = spark.sql("SELECT from_xml(x, 'id variant, name variant, extension variant, birthDate variant') AS s FROM t").collect()[0][0]
for f in ["id", "name", "extension", "birthDate"]:
    print(f"   {f}: {r[f]}")
