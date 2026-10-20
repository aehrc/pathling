"""Runs the four empty-value fixtures through the ported original spike, alone and in a batch with a Patient that
has the same elements populated, and prints what is stored and what is written back."""
import json
import sys

sys.path.insert(0, sys.argv[1] + "/scripts")
from pathling import PathlingContext  # noqa: E402
from spike_routes import ROUTES  # noqa: E402

out_dir = sys.argv[2]
pc = PathlingContext.create(enable_terminology=False, enable_extensions=True)
spark = pc.spark
spark.sparkContext.setLogLevel("ERROR")
route = ROUTES["orig"](pc)

FIXTURES = {
    "c2-null": '{"resourceType":"Patient","id":"p4","active":null,"gender":"male"}',
    "c2-empty-obj": '{"resourceType":"Patient","id":"p5","maritalStatus":{},"gender":"male"}',
    "c2-empty-arr": '{"resourceType":"Patient","id":"p6","identifier":[],"gender":"male"}',
    "c2-empty-string": '{"resourceType":"Patient","id":"p7","name":[{"family":""}],"gender":"male"}',
}
FULL = ('{"resourceType":"Patient","id":"full","active":true,"maritalStatus":{"text":"M"},'
        '"identifier":[{"value":"1"}],"name":[{"family":"Smith"}],"gender":"female"}')
FIELD = {"c2-null": "active", "c2-empty-obj": "maritalStatus", "c2-empty-arr": "identifier",
         "c2-empty-string": "name"}

for name, doc in FIXTURES.items():
    for mode, docs in (("alone", [doc]), ("batch", [doc, FULL])):
        raw = spark.createDataFrame([(d,) for d in docs], "value string")
        try:
            enc = route.fit("json", raw, "Patient")
            path = f"{out_dir}/orig-empty-{name}-{mode}"
            enc.write.mode("overwrite").parquet(path)
            st = spark.read.parquet(path)
            field = FIELD[name]
            stype = st.schema[field].dataType.simpleString() if field in st.columns else "(no column)"
            stored = st.filter(st.id != "full").select(field).first()[0] if field in st.columns else None
            back = json.loads(route.decode("json", st.filter(st.id != "full"), "Patient").first()[0])
            print(f"{name:16} {mode:5} column {field}: {stype:40.40} stored {stored!r:28.28} "
                  f"written back {json.dumps(back.get(field, '<absent>'))}")
        except Exception as e:  # noqa: BLE001
            print(f"{name:16} {mode:5} ERROR " + " | ".join(l for l in str(e).splitlines() if "Exception" in l or "Error" in l)[:400])
