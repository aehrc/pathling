"""Compares two ResultsDump files view by view. Usage: compare.py results-4.0.txt results-4.3.txt."""
import sys


def load(path):
    views, order, cur = {}, [], None
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        tag, _, rest = line.partition("\t")
        if tag == "VIEW":
            cur = {"schema": None, "rows": [], "error": None}
            views[rest] = cur
            order.append(rest)
        elif tag == "SCHEMA":
            cur["schema"] = rest
        elif tag == "ROW":
            cur["rows"].append(rest)
        elif tag == "ERROR":
            cur["error"] = rest
    return views, order


a, order = load(sys.argv[1])
b, _ = load(sys.argv[2])
equal = 0
for label in order:
    x, y = a[label], b.get(label)
    problems = []
    if y is None:
        problems.append("missing on second version")
    else:
        if x["error"] != y["error"]:
            problems.append("error differs: %s vs %s" % (x["error"], y["error"]))
        if x["schema"] != y["schema"]:
            problems.append("schema differs: A %s B %s" % (x["schema"], y["schema"]))
        if x["rows"] != y["rows"]:
            sa, sb = set(x["rows"]), set(y["rows"])
            problems.append(
                "rows differ (%d vs %d): onlyA=%s onlyB=%s"
                % (len(x["rows"]), len(y["rows"]), sorted(sa - sb)[:5], sorted(sb - sa)[:5])
            )
    if problems:
        print("DIFFERENT\t%s\t%s" % (label, "; ".join(problems)))
    else:
        equal += 1
        print("EQUAL\t%s\trows=%d%s" % (label, len(x["rows"]), " (error on both)" if x["error"] else ""))
print("SUMMARY\t%d views, %d equal, %d different" % (len(order), equal, len(order) - equal))
