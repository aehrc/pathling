"""Does folding a binary merge-cast give the same result as one flat n-way merge?

a: struct<id,family>   b: struct<id,given>   c: struct<id,period<start>>
Canonical (definition) order: id, family, given, period.
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.sql("""
  select array(named_struct('id','1','family','Smith')) as a,
         array(named_struct('id','2','given',array('Jo'))) as b,
         array(named_struct('id','3','period',named_struct('start','2020'))) as c
""")

N = "cast(null as string)"
NG = "cast(null as array<string>)"
NP = "cast(null as struct<start:string>)"


def proj(col, has):
    """By-name projection of `col` into canonical order id,family,given,period."""
    f = [f"'id', x.id",
         f"'family', {'x.family' if 'family' in has else N}",
         f"'given', {'x.given' if 'given' in has else NG}",
         f"'period', {'x.period' if 'period' in has else NP}"]
    return f"transform({col}, x -> named_struct({', '.join(f)}))"


# Flat: all three projected into the full merged type at once.
flat = f"concat({proj('a', {'family'})}, {proj('b', {'given'})}, {proj('c', {'period'})})"

# Fold: (a,b) merged to struct<id,family,given>, then that merged with c.
def proj2(col, has):
    f = [f"'id', x.id",
         f"'family', {'x.family' if 'family' in has else N}",
         f"'given', {'x.given' if 'given' in has else NG}"]
    return f"transform({col}, x -> named_struct({', '.join(f)}))"

ab = f"concat({proj2('a', {'family'})}, {proj2('b', {'given'})})"
fold = (f"concat({proj('ab', {'family', 'given'})}, {proj('c', {'period'})})")

flat_df = df.selectExpr(f"{flat} as x")
fold_df = df.selectExpr(f"{ab} as ab", "c").selectExpr(f"{fold} as x")

print("flat type:", flat_df.schema["x"].dataType.simpleString())
print("fold type:", fold_df.schema["x"].dataType.simpleString())
print("types equal:", flat_df.schema["x"].dataType == fold_df.schema["x"].dataType)
print("values equal:", flat_df.collect()[0][0] == fold_df.collect()[0][0])
print("value:", flat_df.collect()[0][0])
spark.stop()
