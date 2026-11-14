"""Drops the executed-plan text and most scan metrics from the result JSON, except the plan text of
the cases the write-up discusses, so that the committed evidence stays small. Usage: trim_results.py <results dir>"""
import glob
import json
import sys

KEEP = ("1b", "3c", "`name.period.start`")


def walk(o, keep):
    if isinstance(o, dict):
        if "case" in o:
            keep = o["case"].startswith(KEEP)
        if "executedPlan" in o and not keep:
            o["executedPlan"] = "(trimmed)"
        if "scanMetrics" in o:
            o["scanMetrics"] = [{k: v for k, v in m.items() if "size of files" in k or "number of files" in k}
                                for m in o["scanMetrics"]]
        for v in o.values():
            walk(v, keep)
    elif isinstance(o, list):
        for v in o:
            walk(v, keep)


for f in glob.glob(sys.argv[1] + "/*/*.json"):
    d = json.load(open(f))
    walk(d, False)
    json.dump(d, open(f, "w"), separators=(",", ":"))
