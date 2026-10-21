import json, sys, os, re, glob, shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType
jar = os.path.expanduser("~/.m2/repository/au/csiro/pathling/library-runtime/10.0.0-SNAPSHOT/library-runtime-10.0.0-SNAPSHOT.jar")
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.jars", jar).getOrCreate())
spark.sparkContext.setLogLevel("OFF")
sys.path.insert(0, sys.argv[1]); OUT = sys.argv[2]
from pathling import PathlingContext
pc = PathlingContext.create(spark, enable_terminology=False)

def short(e):
    m = str(e)
    hits = [l.strip() for l in m.splitlines() if re.search(r"(Exception:|Error:)", l) and "collectToPython" not in l]
    return (hits[0] if hits else m.splitlines()[0])[:200]

base_res = {"resourceType": "Patient", "id": "p1",
            "name": [{"family": "Smith", "given": ["Ann"]}], "maritalStatus": {"text": "M"}}
enc = lambda r: pc.encode(spark.createDataFrame([(json.dumps(r),)], ["v"]), "Patient")
base = enc(base_res)
name_t = base.schema["name"].dataType            # array<struct>
name_el = name_t.elementType
ms_t = base.schema["maritalStatus"].dataType
given_t = name_el["given"].dataType
empty_name = F.from_json(F.lit("{}"), name_el)    # present struct, all fields null
empty_ms = F.from_json(F.lit("{}"), ms_t)

def with_given(expr):
    # Replace given in the first name, keeping family.
    return F.array(F.col("name")[0].withField("given", F.expr(expr).cast(given_t)))

FOREIGN = [
    ("baseline", None),
    ("name = null", F.lit(None).cast(name_t)),
    ("name = []", F.array().cast(name_t)),
    ("name = [null]", F.array(F.lit(None).cast(name_el))),
    ("name = [{}]", F.array(empty_name)),
    ("name = [Smith, {}]", F.array(F.col("name")[0], empty_name)),
    ("name = [null, Smith]", F.array(F.lit(None).cast(name_el), F.col("name")[0])),
    ("given = []", with_given("array()")),
    ("given = [null]", with_given("array(cast(null as string))")),
    ("given = [null, Ann]", with_given("array(cast(null as string), 'Ann')")),
    ("given = ['']", with_given("array('')")),
    ("maritalStatus = {}", "MS"),
]
PATHS = ["name.exists()", "name.count()", "name.family.count()", "name.given.exists()",
         "name.given.count()", "name.given.first()", "name.where(family.empty()).count()",
         "maritalStatus.exists()", "maritalStatus.empty()"]

def run(label, df):
    ds = pc.read.datasets({"Patient": df})
    cols = [{"path": p, "name": f"c{i}"} for i, p in enumerate(PATHS)]
    try:
        r = ds.view("Patient", select=[{"column": cols}]).collect()[0]
        vals = dict(zip(PATHS, tuple(r)))
    except Exception as e:
        vals = {"ERROR": short(e)}
    try:
        fe = ds.view("Patient", select=[{"column": [{"path": "id", "name": "id"}]},
                                        {"forEach": "name", "column": [{"path": "family", "name": "f"}]}]).count()
        feon = ds.view("Patient", select=[{"column": [{"path": "id", "name": "id"}]},
                                          {"forEachOrNull": "name", "column": [{"path": "family", "name": "f"}]}]).count()
    except Exception as e:
        fe = feon = "ERR " + short(e)
    d = os.path.join(OUT, re.sub(r"\W+", "_", label))
    shutil.rmtree(d, ignore_errors=True)
    try:
        ds.write.ndjson(d)
        files = glob.glob(os.path.join(d, "**", "*.ndjson"), recursive=True)
        exported = open(files[0]).read().strip() if files else "(no file)"
    except Exception as e:
        exported = "ERR " + short(e)
    print(f"=== {label}")
    for k, v in vals.items(): print(f"  {k:38s} {v!r}")
    print(f"  {'rows forEach / forEachOrNull':38s} {fe} / {feon}")
    print(f"  {'export':38s} {exported}")

print("##### WRITTEN DIRECTLY, AS A FOREIGN WRITER COULD")
for label, col in FOREIGN:
    if col is None: df = base
    elif isinstance(col, str): df = base.withColumn("maritalStatus", empty_ms)
    else: df = base.withColumn("name", col)
    run(label, df)

print("##### THROUGH JSON INGEST")
JSONS = [
    ("json name: []", {"name": []}),
    ("json name: [{}]", {"name": [{}]}),
    ("json name: [null]", {"name": [None]}),
    ("json maritalStatus: {}", {"maritalStatus": {}}),
    ("json maritalStatus: null", {"maritalStatus": None}),
    ("json given: [null]", {"name": [{"family": "Smith", "given": [None]}]}),
    ("json given: []", {"name": [{"family": "Smith", "given": []}]}),
    ("json birthDate: ''", {"birthDate": ""}),
    ("json _given only", {"name": [{"family": "Smith", "_given": [{"extension": [{"url": "u", "valueString": "E"}]}]}]}),
]
for label, extra in JSONS:
    r = {"resourceType": "Patient", "id": "p1"}; r.update(extra)
    try:
        df = enc(r)
        print(f"--- {label}: stored " + json.dumps(df.drop("__layout_v1").toJSON().collect()[0]) + "  schema=" + ",".join(f.name + ":" + f.dataType.simpleString()[:60] for f in df.schema.fields if f.name in ("name","maritalStatus","birthDate")))
        run(label, df)
    except Exception as e:
        print(f"=== {label}\n  ingest FAILED {short(e)}")
