"""Writes a small NDJSON file of Patients with several names each, a date under every name, and
repeating givens, for the repeating-primitive CPU spike. Usage: gen_patients.py <out> [n]."""
import json
import random
import sys

random.seed(7)
n = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
starts = ["1999", "2003-05", "1985-11-23", "2010-07-04T10:15:30Z", "1970-01-01", "2021-12",
          "1962-03-30T14:00:00+10:00"]
with open(sys.argv[1], "w") as f:
    for i in range(n):
        names = []
        for k in range(random.randint(3, 6)):
            nm = {"use": "official",
                  "given": [f"Given{random.randint(0, 999)}" for _ in range(random.randint(1, 3))]}
            if random.random() < 0.9:
                nm["family"] = f"Family{random.randint(0, 9999)}"
            if random.random() < 0.9:
                nm["period"] = {"start": random.choice(starts)}
            if random.random() < 0.1:
                nm["_given"] = [None] * (len(nm["given"]) - 1) + [
                    {"extension": [{"url": "http://example.org/nick", "valueString": "x"}]}]
            names.append(nm)
        f.write(json.dumps({"resourceType": "Patient", "id": f"p{i}", "name": names}) + "\n")
