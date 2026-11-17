"""Writes Observation.ndjson: N observations whose valueQuantity (mg) rises with the id, each with an
element id and an extension inside the quantity."""
import json
import sys

n = int(sys.argv[2])
with open(sys.argv[1] + "/Observation.ndjson", "w") as f:
    for i in range(n):
        f.write(
            json.dumps(
                {
                    "resourceType": "Observation",
                    "id": "o%d" % i,
                    "status": "final",
                    "code": {"text": "c"},
                    "valueQuantity": {
                        "id": "q%d" % i,
                        "extension": [{"url": "http://x/e", "valueString": "e%d" % i}],
                        "value": round(i * 0.001, 3),
                        "unit": "mg",
                        "system": "http://unitsofmeasure.org",
                        "code": "mg",
                    },
                }
            )
            + "\n"
        )
