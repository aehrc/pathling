"""Does parse_json (VARIANT) keep what the JSON reader loses: null vs absent, JSON token type, {} , decimal text, keys?"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
doc = '{"active":null,"s":"5","n":5,"d":1.50,"big":12345678901234567890.123456789012345678901234,"e":1.0e2,"o":{},"a":[],"name":[{"family":"X","nickname":"Y"}]}'
spark.createDataFrame([(doc,)], "j string").createOrReplaceTempView("t")
q = """
SELECT
  variant_get(v, '$.active') IS NULL                    AS active_sql_null,
  is_variant_null(variant_get(v, '$.active'))           AS active_is_json_null,
  variant_get(v, '$.missing') IS NULL                   AS missing_sql_null,
  is_variant_null(variant_get(v, '$.missing'))          AS missing_is_json_null,
  schema_of_variant(variant_get(v, '$.s'))              AS s_type,
  schema_of_variant(variant_get(v, '$.n'))              AS n_type,
  schema_of_variant(variant_get(v, '$.d'))              AS d_type,
  CAST(variant_get(v, '$.d') AS STRING)                 AS d_text,
  schema_of_variant(variant_get(v, '$.big'))            AS big_type,
  CAST(variant_get(v, '$.big') AS STRING)               AS big_text,
  schema_of_variant(variant_get(v, '$.e'))              AS e_type,
  CAST(variant_get(v, '$.e') AS STRING)                 AS e_text,
  schema_of_variant(variant_get(v, '$.o'))              AS o_type,
  schema_of_variant(variant_get(v, '$.a'))              AS a_type,
  schema_of_variant(variant_get(v, '$.name[0]'))        AS name0_keys
FROM (SELECT parse_json(j) AS v FROM t)
"""
for k, val in spark.sql(q).collect()[0].asDict().items():
    print(f"{k:22} {val}")
print("== object keys via variant_explode")
spark.sql("SELECT key FROM (SELECT parse_json(j) AS v FROM t), LATERAL variant_explode(variant_get(v, '$.name[0]'))").show()
