"""Spike 08 phase 2: a messy variant of the JSON benchmark data, for A+'s worst case.

One document per file, the one in the middle, gets a shape error at an intermediate node, where the
definitions declare an array and the document gives a single object:
  Patient      name          [ {...} ]  ->  {...}
  Condition    code.coding   [ {...} ]  ->  {...}
  Encounter    type          [ {...} ]  ->  {...}
  Observation  code.coding   [ {...} ]  ->  {...}
Inference then widens that node to text for every row of the file (widening_probe.py), so a route that reads
with the inferred schema has to resolve it per row. Every other line is copied byte for byte.
Usage: make_messy.py <sourceDir> <outDir>
"""
import json, os, sys

source, out = sys.argv[1:]
os.makedirs(out, exist_ok=True)
PATHS = {"Patient": ["name"], "Condition": ["code", "coding"], "Encounter": ["type"], "Observation": ["code", "coding"]}
for t, path in PATHS.items():
    lines = open(f"{source}/{t}.ndjson").read().splitlines()
    target = len(lines) // 2
    while True:
        doc = json.loads(lines[target])
        node = doc
        for k in path[:-1]:
            node = node.get(k, {})
        if isinstance(node.get(path[-1]), list) and node[path[-1]]:
            node[path[-1]] = node[path[-1]][0]
            lines[target] = json.dumps(doc, separators=(",", ":"))
            break
        target += 1
    with open(f"{out}/{t}.ndjson", "w") as f:
        f.write("\n".join(lines) + "\n")
    print(t, "line", target + 1, "id", doc["id"], "path", ".".join(path))
