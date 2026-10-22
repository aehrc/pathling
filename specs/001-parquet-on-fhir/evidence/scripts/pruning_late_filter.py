"""Why the traversal removes nulls, and whether removing them after the field read still prunes."""
import re, tempfile
from pyspark.sql import SparkSession
s = SparkSession.builder.master('local[1]').config('spark.sql.maxMetadataStringLength', '100000').getOrCreate()
s.sparkContext.setLogLevel('ERROR')
d = tempfile.mkdtemp()
s.sql("select array(named_struct('family','f','given',array('a','b'),'big','x'), named_struct('family',cast(null as string),'given',cast(null as array<string>),'big','y')) name").write.parquet(d + '/t')
df = s.read.parquet(d + '/t')
for q in ['flatten(name.given)', 'flatten(filter(name.given, x -> x is not null))', 'filter(name.family, x -> x is not null)', 'size(name.family)', 'size(filter(name.family, x -> x is not null))']:
    r = df.selectExpr(q + ' as v')
    plan = r._jdf.queryExecution().executedPlan().toString()
    print(q, '|', re.findall(r'ReadSchema: (struct<.*?>)\s*$', plan, re.M)[-1], '|', r.first()[0])
