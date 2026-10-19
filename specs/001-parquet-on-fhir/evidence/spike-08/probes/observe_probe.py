"""Can Dataset.observe carry a bounded sample of findings to the driver, not only counts?

Decision 133 delivers a summary through Dataset.observe during the write. This tries the public-API routes to
a sample of findings within the same action, with a noop write standing in for the sink:
  1. collect_list, then slice: accepted or not, and whether it is bounded;
  2. min(struct(...)) per kind: one deterministic example per kind, bounded by the number of kinds;
  3. k examples per kind by hashing into k buckets, each a min(struct): bounded and deterministic;
  4. max_by: one example per kind, ordered by a key.
The findings are an array of (path, kind, value) per row, as decision 133's _findings column would be.
"""
from pyspark.sql import SparkSession, Observation, functions as F

spark = SparkSession.builder.master("local[4]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
print("spark", spark.version)

# 100,000 rows; every 7th has an invalid date, every 11th an unknown key.
rows = spark.range(100000).select(
    F.concat(F.lit("p"), F.col("id").cast("string")).alias("id"),
    F.when(F.col("id") % 7 == 0, F.lit("1980-13-45")).otherwise(F.lit("1980-01-01")).alias("birthDate"),
    F.when(F.col("id") % 11 == 0, F.lit("bar")).alias("foo"))
finding = F.struct
findings = F.filter(F.array(
    F.when(F.col("birthDate") == "1980-13-45", finding(F.lit("birthDate").alias("path"), F.lit("invalid").alias("kind"), F.col("birthDate").alias("value"))),
    F.when(F.col("foo").isNotNull(), finding(F.lit("foo").alias("path"), F.lit("undescribed").alias("kind"), F.col("foo").alias("value")))),
    lambda f: f.isNotNull())
with_findings = rows.withColumn("_findings", findings)


def attempt(label, *metrics):
    obs = Observation(label)
    try:
        with_findings.observe(obs, *metrics).write.format("noop").mode("overwrite").save()
        result = obs.get
        print(f"\n== {label}: accepted")
        for k, v in result.items():
            text = str(v)
            print(f"   {k}: {text[:400]}{' ...(' + str(len(text)) + ' chars)' if len(text) > 400 else ''}")
    except Exception as e:
        import re
        causes = sorted(set(re.findall(r"\[([A-Z_]+(?:\.[A-Z_]+)?)\]", str(e))))
        print(f"\n== {label}: REJECTED {' '.join(causes)}")
        print("   " + str(e).splitlines()[0][:300])


# Counts, the part decision 133 already relies on.
attempt("counts",
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.size("_findings")).alias("findings"),
        F.count(F.when(F.size("_findings") > 0, 1)).alias("rows_affected"))

# 1. collect_list then slice. The list is built in full on every partition before slicing.
attempt("collect_list + slice",
        F.slice(F.collect_list(F.when(F.size("_findings") > 0, F.struct("id", "_findings"))), 1, 5).alias("sample"),
        F.size(F.collect_list(F.when(F.size("_findings") > 0, F.struct("id", "_findings")))).alias("list_size_before_slice"))

# 2. One example per kind: min over a struct, null where the row has no finding of that kind.
def first_of_kind(kind):
    hit = F.filter("_findings", lambda f: f["kind"] == kind)
    return F.min(F.when(F.size(hit) > 0, F.struct(F.col("id"), hit[0]["path"].alias("path"), hit[0]["value"].alias("value")))).alias(f"example_{kind}")
attempt("min(struct) per kind", first_of_kind("invalid"), first_of_kind("undescribed"))

# 3. k examples per kind, k = 3: one min per hash bucket. Bounded at kinds x k structs.
def k_of_kind(kind, k=3):
    hit = F.filter("_findings", lambda f: f["kind"] == kind)
    bucket = F.pmod(F.xxhash64("id"), F.lit(k))
    return [F.min(F.when((F.size(hit) > 0) & (bucket == b), F.struct(F.col("id"), hit[0]["value"].alias("value")))).alias(f"{kind}_{b}") for b in range(k)]
attempt("k per kind via hash buckets", *(k_of_kind("invalid") + k_of_kind("undescribed")))

# 4. max_by: the example with the greatest key.
attempt("max_by per kind",
        F.max_by(F.struct("id", "_findings"), F.when(F.size(F.filter("_findings", lambda f: f["kind"] == "invalid")) > 0, F.xxhash64("id"))).alias("example_invalid"))

# A deterministic expression that is not an aggregate, for contrast.
attempt("non-aggregate expression", F.col("id").alias("not_an_aggregate"))
