"""Writes Patient.ndjson and Encounter.ndjson without any extensions. The element-level metadata is
spliced in after encoding (Splice.java), because the library's reader does not store the `_x`
groups of primitives on this branch. Usage: gen_ndjson.py <out dir> <patients> <encounters>."""
import json
import os
import random
import sys

out, n_pat, n_enc = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rnd = random.Random(11)
os.makedirs(out, exist_ok=True)


def date(lo, hi):
    y = rnd.randint(lo, hi)
    k = rnd.random()
    if k < 0.1:
        return f"{y}"
    if k < 0.2:
        return f"{y}-{rnd.randint(1, 12):02d}"
    return f"{y}-{rnd.randint(1, 12):02d}-{rnd.randint(1, 28):02d}"


def dtime(lo, hi):
    d = date(lo, hi)
    return d if len(d) < 10 or rnd.random() < 0.5 else d + "T10:15:30Z"


def put(f, o):
    f.write(json.dumps(o, separators=(",", ":")) + "\n")


with open(f"{out}/Patient.ndjson", "w") as f:
    for i in range(n_pat):
        names = []
        for _ in range(max(1, int(rnd.gauss(2.5, 1)))):
            names.append({"use": "official", "family": f"Family{rnd.randint(0, 99999)}",
                          "given": [f"Given{rnd.randint(0, 99999)}" for _ in range(rnd.randint(1, 3))]})
        put(f, {"resourceType": "Patient", "id": f"p{i}", "birthDate": date(1930, 2020), "name": names})
with open(f"{out}/Encounter.ndjson", "w") as f:
    for i in range(n_enc):
        locs = [{"location": {"reference": f"Location/l{rnd.randint(0, 999)}"}, "status": "completed",
                 "period": {"start": dtime(2010, 2025)}} for _ in range(max(1, int(rnd.gauss(2, 0.8))))]
        put(f, {"resourceType": "Encounter", "id": f"e{i}", "status": "finished",
                "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB"},
                "location": locs})
