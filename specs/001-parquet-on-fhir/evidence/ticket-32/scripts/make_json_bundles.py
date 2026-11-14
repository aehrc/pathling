"""Scratch, ticket 32: JSON bundles for the benchmark data, one per patient, one per line, as the XML bundles are.

collection: each entry {resource}, references as in the source (Patient/<id>, Encounter/<id>).
transaction: each entry {fullUrl: urn:uuid:<id>, resource, request}, and every Patient/ or Encounter/
reference to a resource of the same bundle rewritten to urn:uuid:<id>, as tools/ToXmlTxn.java does for XML.
Usage: make_json_bundles.py <jsonDir> <outDir>
"""
import json, os, re, sys
src, out = sys.argv[1:]
os.makedirs(out, exist_ok=True)
by_patient = {}
order = ["Patient", "Condition", "Encounter", "Observation"]
for t in order:
    with open(f"{src}/{t}.ndjson") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            pid = r["id"] if t == "Patient" else r.get("subject", {}).get("reference", "").split("/")[-1]
            by_patient.setdefault(pid, []).append(r)
REF = re.compile(r'"reference":"(Patient|Encounter)/([^"]+)"')
with open(f"{out}/collection.ndjson", "w") as c, open(f"{out}/transaction.ndjson", "w") as x:
    for pid, resources in by_patient.items():
        c.write(json.dumps({"resourceType": "Bundle", "type": "collection",
                            "entry": [{"resource": r} for r in resources]}, separators=(",", ":")) + "\n")
        ids = {f'{r["resourceType"]}/{r["id"]}' for r in resources}
        entries = []
        for r in resources:
            text = json.dumps(r, separators=(",", ":"))
            text = REF.sub(lambda m: '"reference":"urn:uuid:%s"' % m.group(2) if f"{m.group(1)}/{m.group(2)}" in ids else m.group(0), text)
            entries.append('{"fullUrl":"urn:uuid:%s","resource":%s,"request":{"method":"POST","url":"%s"}}' % (r["id"], text, r["resourceType"]))
        x.write('{"resourceType":"Bundle","type":"transaction","entry":[' + ",".join(entries) + "]}\n")
print(len(by_patient), "bundles")
