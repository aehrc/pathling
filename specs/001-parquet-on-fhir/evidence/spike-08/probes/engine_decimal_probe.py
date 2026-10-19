"""What does the engine (FHIRPath over PoF data) do with an invalid stored decimal?

Ticket 31 asks for this to be pinned before principle 7 is applied, so nothing here is fixed. A valid
Observation is encoded, then its stored valueQuantity.value, which the layout keeps as text, is overwritten
with invalid text, as PoF data written by another tool could hold. Each FHIRPath expression is evaluated in a
view of its own, so one failure does not hide another, and the outcome is printed: the values, or the error.
round(), sum() and iif() are not used: this build reports each as an unsupported function.
"""
import hashlib, glob, os, re, sys
from pyspark.sql import functions as F
from pathling import PathlingContext

pc = PathlingContext.create(enable_terminology=False)
spark = pc.spark
spark.sparkContext.setLogLevel("OFF")
print("spark", spark.version)
ivy = spark.conf.get("spark.jars.ivy", "")
for jar in sorted(glob.glob(f"{ivy}/jars/au.csiro.pathling_library-runtime-*.jar")):
    print("pathling jar", os.path.basename(jar), hashlib.sha256(open(jar, "rb").read()).hexdigest())

doc = ('{"resourceType":"Observation","id":"o1","status":"final","code":{"text":"x"},'
       '"valueQuantity":{"value":1.5,"unit":"mg","system":"http://unitsofmeasure.org","code":"mg"},'
       '"component":[{"code":{"text":"a"},"valueQuantity":{"value":2.5,"unit":"mg"}}]}')
encoded = pc.encode(spark.createDataFrame([(doc,)], "value string"), "Observation")
print("stored type of valueQuantity.value:", encoded.schema["valueQuantity"].dataType["value"].dataType.simpleString())
print("stored value:", encoded.select("valueQuantity.value").first()[0])

expressions = [
    "valueQuantity.value",
    "valueQuantity.value.exists()",
    "valueQuantity.value.empty()",
    "valueQuantity.value.toString()",
    "valueQuantity.value > 1",
    "valueQuantity.value = 1.5",
    "valueQuantity.value + 1",
    "valueQuantity.value * 2",
    "valueQuantity.value.count()",
    "valueQuantity.value.convertsToDecimal()",
    "valueQuantity.value.toInteger()",
    "valueQuantity.value.toDecimal()",
    "valueQuantity.value.where($this > 1).exists()",
    "valueQuantity.value > 1 or valueQuantity.value <= 1",
    "valueQuantity > 1 'mg'",
    "valueQuantity.value | component.valueQuantity.value",
    "(valueQuantity.value | component.valueQuantity.value).count()",
]
invalid = ["1.5", "abc", "1.2.3", "", " 1.5", "1.5 ", "1,5", "NaN", "Infinity", "1e400", "1.0e2",
           "123456789012345678901234567890123456789.5"]


def outcome(df, expr):
    try:
        view = pc.read.datasets({"Observation": df}).view(
            resource="Observation", select=[{"column": [{"path": expr, "name": "r", "collection": True}]}])
        return "ok    " + str([r.r for r in view.collect()])
    except Exception as e:
        causes = sorted(set(re.findall(r"\[([A-Z_]+(?:\.[A-Z_]+)*)\]", str(e))))
        # The first line names only the py4j call; the Java exception's own message follows it.
        lines = [l.strip() for l in str(e).splitlines() if l.strip()]
        first = next((l for l in lines if re.match(r"^(:\s*)?[a-z][\w.]*(Exception|Error)\b", l)), lines[0] if lines else "")
        return "ERROR " + " ".join(causes) + " | " + first[:220]


for text in invalid:
    df = encoded.withColumn("valueQuantity", F.col("valueQuantity").withField("value", F.lit(text)))
    print(f"\n== stored valueQuantity.value = {text!r}")
    for expr in expressions:
        print(f"   {expr:58} {outcome(df, expr)}")
