"""Writes a scaled Patient.ndjson for the engine views: the shape of the OSS spike's patients, with
unique extension payloads so that the stored data does not compress away, and the two edge patients.

Usage: gen_patients.py <out file> <count>
"""
import json
import random
import sys

rnd = random.Random(42)


def ext(tag):
    return [{"url": f"http://ex/{i}", "valueString": f"{tag}-{rnd.getrandbits(64):x}-{i}"}
            for i in range(1, 6)]


def patients(n):
    out = []
    for i in range(n):
        y = 1950 + i % 70
        out.append({
            "resourceType": "Patient", "id": f"p{i}", "birthDate": f"{y}-01-01",
            "_birthDate": {"extension": ext("bd")},
            "name": [
                {"extension": ext("n"), "family": f"F{i}", "_family": {"extension": ext("f")},
                 "given": [f"a{i}", f"b{i}"],
                 "period": {"start": f"{y}-02-02", "_start": {"extension": ext("ps")},
                            "end": f"{y + 1}-02-02"}},
                {"use": "nickname", "given": [f"n{i}"], "extension": ext("n2")},
            ],
            "address": [{"city": f"C{i}", "line": [f"l{i}"], "extension": ext("a"),
                         "period": {"start": f"{y}-03-03"}}],
        })
    out.append({"resourceType": "Patient", "id": "e1", "name": [{"extension": ext("only")}]})
    out.append({"resourceType": "Patient", "id": "e2"})
    return out


if __name__ == "__main__":
    with open(sys.argv[1], "w") as f:
        f.write("\n".join(json.dumps(p) for p in patients(int(sys.argv[2]))) + "\n")
