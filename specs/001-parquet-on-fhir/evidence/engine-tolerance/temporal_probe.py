import re
import json, sys, os
from pyspark.sql import SparkSession
jar = os.path.expanduser("~/.m2/repository/au/csiro/pathling/library-runtime/10.0.0-SNAPSHOT/library-runtime-10.0.0-SNAPSHOT.jar")
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.jars", jar).getOrCreate())
spark.sparkContext.setLogLevel("OFF")
sys.path.insert(0, sys.argv[1])
from pathling import PathlingContext
pc = PathlingContext.create(spark, enable_terminology=False)

def short(e):
    m = str(e)
    hits = [l.strip() for l in m.splitlines() if re.search(r"(Illegal|DateTime|Exception:)", l) and "collectToPython" not in l]
    return (hits[0] if hits else m.splitlines()[0])[:240]

CASES = [
    ("Patient", "birthDate", "2020-13-45", "@2000-01-01"),
    ("Patient", "birthDate", "2020-02-30", "@2000-01-01"),
    ("Patient", "birthDate", "2020-01-01T10:00:00Z", "@2000-01-01"),
    ("Patient", "birthDate", "garbage", "@2000-01-01"),
    ("Patient", "deceasedDateTime", "2020-01-01T10:00:00", "@2000-01-01T00:00:00Z"),
    ("Patient", "deceasedDateTime", "2020-01-01T25:00:00Z", "@2000-01-01T00:00:00Z"),
    ("Observation", "valueTime", "25:61:00", "@T10:00:00"),
]
for rt, field, val, lit in CASES:
    res = {"resourceType": rt, "id": "r1", field: val}
    if rt == "Observation":
        res.update({"status": "final", "code": {"text": "x"}})
    path = field.replace("deceasedDateTime", "deceased.ofType(dateTime)").replace("valueTime", "value.ofType(time)")
    print(f"=== {rt}.{field} = {val!r}")
    try:
        enc = pc.encode(spark.createDataFrame([(json.dumps(res),)], ["v"]), rt)
        stored = enc.select(field).collect()[0][0]
        print(f"  ingest: stored {stored!r}")
    except Exception as e:
        print("  ingest: FAILED", short(e)); continue
    ds = pc.read.datasets({rt: enc})
    queries = [
        ("select value", dict(select=[{"column": [{"path": path, "name": "v"}]}])),
        ("select exists()", dict(select=[{"column": [{"path": f"{path}.exists()", "name": "v"}]}])),
        ("select = same text", dict(select=[{"column": [{"path": f"{path}.toString() = '{val}'", "name": "v"}]}])),
        ("select comparison", dict(select=[{"column": [{"path": f"{path} > {lit}", "name": "v"}]}])),
        ("where comparison", dict(select=[{"column": [{"path": "id", "name": "id"}]}], where=[{"path": f"{path} > {lit}"}])),
        ("select equality", dict(select=[{"column": [{"path": f"{path} = {lit}", "name": "v"}]}])),
        ("select toDate/ToDateTime", dict(select=[{"column": [{"path": f"{path}.toString().toDateTime()", "name": "v"}]}])),
    ]
    for label, q in queries:
        try:
            rows = ds.view(rt, **q).collect()
            print(f"  {label:26s} -> {[tuple(r) for r in rows]}")
        except Exception as e:
            print(f"  {label:26s} -> ERROR {short(e)}")
