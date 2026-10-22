"""Does any Spark setting let nested schema pruning through a lambda over an array of structures?"""
import re, tempfile, os
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").config("spark.sql.maxMetadataStringLength","100000").config("spark.sql.adaptive.enabled","false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version)
d = tempfile.mkdtemp()
spark.sql("""select 'p' id, array(named_struct('use','u','family','f','given',array('a','b'),
  'period',named_struct('start','2000','end','2010'),
  'extension',array(named_struct('url','x','valueString','y')))) name""").write.parquet(d+"/p")
df = spark.read.parquet(d+"/p")
def rs(q):
    m = re.findall(r"ReadSchema: (struct<.*?>)\s*$", df.selectExpr(q)._jdf.queryExecution().executedPlan().toString(), re.M)
    return m[-1] if m else "?"
Q = {
 "engine today: filter(name, notnull).family": "filter(name, x -> x is not null).family",
 "filter after:  filter(name.family, notnull)": "filter(name.family, x -> x is not null)",
 "transform(name, x -> x.family)": "transform(name, x -> x.family)",
 "transform(filter(name, notnull), x -> x.family)": "transform(filter(name, x -> x is not null), x -> x.family)",
 "filter(name, x -> x.family is not null).family": "filter(name, x -> x.family is not null).family",
 "plain name.family": "name.family",
 "name.period.start": "name.period.start",
}
confs = [None,
 ("spark.sql.optimizer.nestedSchemaPruning.enabled","false"),
 ("spark.sql.optimizer.expression.nestedPruning.enabled","false"),
 ("spark.sql.parquet.enableNestedColumnVectorizedReader","false"),
 ("spark.sql.optimizer.excludedRules","")]
for c in confs:
    if c: spark.conf.set(*c)
    print("\n== conf:", c or "defaults")
    for k,q in Q.items(): print(f"  {k:52s} {rs(q)}")
    if c: spark.conf.unset(c[0])
