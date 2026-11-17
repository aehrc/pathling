"""Pivots the Spark reads logs into a table: case, then footer bytes and leaves for p and z.

Usage: table.py <reads log> <variant m0|m1> [--leaves].
"""
import sys

rows = {}
for line in open(sys.argv[1]):
    if not line.startswith("ROW|"):
        continue
    parts = line.rstrip("\n").split("|")
    variant, layout, case = parts[1].split(" ", 2)
    if variant != sys.argv[2]:
        continue
    if parts[2] == "ERROR":
        val = ("ERR", parts[3][:60], "")
    else:
        b = int(parts[2].split("=")[1])
        leaves = parts[-1][len("leaves="):].split(",")
        val = (f"{b:,}", "", leaves)
    rows.setdefault(case, {})[layout] = val
for case, d in rows.items():
    cells = []
    for lay in ("p", "z"):
        v = d.get(lay)
        cells.append("-" if v is None else (v[0] if v[0] != "ERR" else "does not resolve"))
    print(f"| {case} | {cells[0]} | {cells[1]} |")
    if "--leaves" in sys.argv:
        for lay in ("p", "z"):
            v = d.get(lay)
            if v and v[0] != "ERR":
                print(f"|   {lay} leaves | {', '.join(v[2])[:400]} | |")
