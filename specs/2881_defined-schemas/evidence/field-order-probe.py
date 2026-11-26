"""Probe whether Parquet, Delta and catalog tables accept batches that differ only in field order.

Run with PySpark 4.0.x: `python field-order-probe.py`. Delta is fetched through
`spark.jars.packages`, so the first run needs network access to Maven Central.

Batch A has its fields in canonical order. Batch B has the same fields reordered at the top level,
inside a struct and inside an array of structs. The swapped fields share a type, so a positional
mix-up would be silent rather than an error: each printed row must read (s, c, f, g) for its own
batch letter.
"""

import shutil
import tempfile

from pyspark.sql import SparkSession

root = tempfile.mkdtemp()

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.sql.warehouse.dir", f"{root}/warehouse")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

A = spark.sql(
    """select '1' as id, named_struct('system','sA','code','cA') as coding,
       array(named_struct('family','fA','given','gA')) as name"""
)
B = spark.sql(
    """select named_struct('code','cB','system','sB') as coding,
       array(named_struct('given','gB','family','fB')) as name, '2' as id"""
)
# A with a field added inside the struct, to show where schema evolution puts it.
A_EVOLVED = spark.sql(
    """select '3' as id, named_struct('system','s3','version','v3','code','c3') as coding,
       array(named_struct('family','f3','given','g3')) as name"""
)
print("A schema:", A.schema.simpleString())
print("B schema:", B.schema.simpleString())


def show(label, df):
    print(f"--- {label}: {df.schema.simpleString()}")
    for r in df.orderBy("id").collect():
        print(
            "   ",
            r.id,
            (r.coding.system, r.coding.code, r.name[0].family, r.name[0].given),
        )


def attempt(label, run):
    try:
        run()
    except Exception as e:  # noqa: BLE001
        print(
            f"--- {label}: FAILED: {type(e).__name__}: {str(e).strip().splitlines()[0][:300]}"
        )


def path(name):
    return f"{root}/{name}"


# 1. Acceptance: a table first written with A, then given B.
A.write.parquet(path("pq"))
B.write.mode("append").parquet(path("pq"))
attempt(
    "parquet append, read",
    lambda: show("parquet append, read", spark.read.parquet(path("pq"))),
)
attempt(
    "parquet append, read with mergeSchema",
    lambda: show(
        "parquet append, read with mergeSchema",
        spark.read.option("mergeSchema", "true").parquet(path("pq")),
    ),
)


def delta_case(label, write):
    p = path(label.replace(" ", "_"))
    A.write.format("delta").save(p)

    def run():
        write(p)
        show(label, spark.read.format("delta").load(p))

    attempt(label, run)


def merge(source):
    def write(p):
        source.createOrReplaceTempView("src")
        spark.sql(
            f"""merge into delta.`{p}` t using src s on t.id = s.id
            when matched then update set * when not matched then insert *"""
        )

    return write


delta_case("delta append", lambda p: B.write.format("delta").mode("append").save(p))
delta_case(
    "delta append with mergeSchema",
    lambda p: (
        B.write.format("delta").mode("append").option("mergeSchema", "true").save(p)
    ),
)
delta_case("delta merge, insert", merge(B))
delta_case("delta merge, update", merge(B.selectExpr("coding", "name", "'1' as id")))

# 2. Stored order: B written first, then A.
B.write.parquet(path("pq_b_first"))
A.write.mode("append").parquet(path("pq_b_first"))
show("parquet B first, read", spark.read.parquet(path("pq_b_first")))
show(
    "parquet B first, read with mergeSchema",
    spark.read.option("mergeSchema", "true").parquet(path("pq_b_first")),
)
B.write.format("delta").save(path("delta_b_first"))
A.write.format("delta").mode("append").save(path("delta_b_first"))
show(
    "delta B first, A appended", spark.read.format("delta").load(path("delta_b_first"))
)

# 3. Schema evolution: a field added inside a struct.
A.write.format("delta").save(path("delta_evolved"))
A_EVOLVED.write.format("delta").mode("append").option("mergeSchema", "true").save(
    path("delta_evolved")
)
show("delta evolved", spark.read.format("delta").load(path("delta_evolved")))

# 4. A catalog table that is not Delta, appended with saveAsTable, as CatalogSink appends to one.
A.write.format("parquet").option("path", path("catalog_t")).saveAsTable("catalog_t")
attempt(
    "catalog parquet table, B appended",
    lambda: (
        B.write.mode("append").format("parquet").saveAsTable("catalog_t"),
        show("catalog parquet table, B appended", spark.table("catalog_t")),
    ),
)
spark.sql("drop table catalog_t")

# 5. The rejected alternative: Dataset.to reorders a canonical batch into another order by name,
# at the top level and inside structs, arrays of structs and maps of structs.
canonical = spark.sql(
    """select '1' as id, named_struct('system','s','code','c') as coding,
       array(named_struct('family','f','given','g')) as name,
       map('k', named_struct('system','ms','code','mc')) as m"""
)
target = spark.sql(
    """select array(named_struct('given','x','family','x')) as name,
       map('k', named_struct('code','x','system','x')) as m,
       named_struct('code','x','system','x') as coding, 'x' as id"""
).schema


def reorder():
    out = canonical.to(target)
    r = out.collect()[0]
    print(
        f"--- Dataset.to: {out.schema.simpleString()} equal to target: {out.schema == target}"
    )
    print(
        "   ",
        r.coding.system,
        r.coding.code,
        r.name[0].family,
        r.name[0].given,
        r.m["k"].system,
        r.m["k"].code,
    )


attempt("Dataset.to", reorder)

shutil.rmtree(root)
spark.stop()
