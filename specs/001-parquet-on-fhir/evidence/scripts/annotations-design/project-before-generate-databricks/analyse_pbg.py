"""Tables for the compute-first run: classes against the rule-on result of lambda-pruning-databricks."""
import json
import re
import sys

R = sys.argv[1] if len(sys.argv) > 1 else "results/pbg1-photon"
E3 = sys.argv[2] if len(sys.argv) > 2 else "../lambda-pruning-databricks/results/engine3-photon"


def load(p):
    return json.load(open(p))


def one(info):
    return "; ".join(info["reads"]) if isinstance(info["reads"], list) else info["reads"]


def norm(s):
    return s.replace(" (whole)", "")


def parts(s):
    return set(x.strip() for x in norm(s).split(","))


for variant in ["parquet", "delta-dv"]:
    pb = load(f"{R}/pbg-{variant}.json")
    e3 = {v["case"]: v for v in load(f"{E3}/engine-{variant}.json")["views"]}
    print(f"\n### {variant}  (rewrite totals applied/nothingToDo/failed: {pb['rewriteTotals(applied,nothingToDo,failed)']}, "
          f"scan classes {set(c for v in pb['views'] if 'error' not in v for c in v['rewritten']['info']['scanClasses'])})\n")
    print("| View | Orig read | Compute-first read | Rule-on read (earlier) | Orig bytes | CF bytes | Rule-on bytes | Class | Equal | Project kept | prune_struct_fields | Rewrites |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    counts = {}
    for v in pb["views"]:
        if "error" in v:
            print(f"| {v['case']} | ERROR {v['error'][:200]} |")
            continue
        o, r = v["original"], v["rewritten"]
        on = e3[v["case"]]["on"]
        ro, rr, ron = one(o["info"]), one(r["info"]), one(on["info"])
        cls = "A" if parts(rr) == parts(ron) else ("C" if parts(rr) == parts(ro) else "B")
        counts[cls] = counts.get(cls, 0) + 1
        gens = ",".join(r["optimisedGenerates"])
        psf = f"opt:{len(r['pruneStructFieldsOptimised'])} exec:{len(r['pruneStructFieldsExecuted'])}"
        print(f"| {v['case'].replace('|', chr(92)+'|')} | {ro} | {rr} | {ron} | {o['bytesRead']} | {r['bytesRead']} | {on['bytesRead']} | {cls} | "
              f"{'yes' if v['resultsEqual'] else '**NO**'} ({o['rowCount']}) | {gens} | {psf} | {v['rewriteCounters(applied,nothingToDo,failed)']} |")
    print("\nClasses:", counts)
    print("Stable plan/read:", all(v.get('planStable') and v.get('readStable') for v in pb['views'] if 'error' not in v))
