"""Summarises the matrix JSON of one run against the go/no-go checks.

Usage: analyse.py <matrix.json> [-v]
"""
import json
import sys

d = json.load(open(sys.argv[1]))
verbose = "-v" in sys.argv
print("runtime:", {k: d["runtime"][k] for k in ("dbr", "sparkVersion", "java")}, "conf:", d["conf"])
print("relation classes:", json.dumps(d["relationClasses"]))
print("bail reasons:", d["bailReasons"][:3])
tot = {"cases": 0, "errors": 0, "unequal": 0, "bail_on_noncontrol": 0, "bail_control": 0,
       "pruned": 0, "should_prune_but_not": [], "bytes_not_lower": [], "unstable": [],
       "foreign_read_on": [], "scan_classes": set()}
for k in d["results"]:
    f = k["table"]
    print(f"\n== {k['kind']}  dv_added={f.get('deletionVectorsAdded')} props={f.get('properties')} "
          f"cluster={f.get('clusteringColumns')}")
    for c in k["cases"]:
        tot["cases"] += 1
        name = c["case"][:3].strip()
        if "error" in c:
            tot["errors"] += 1
            print("  ERROR", name, c["error"][:150])
            continue
        off, on, on2 = c["off"], c["on"], c["on2"]
        bail_on = on["ruleCounters"][3] + on2["ruleCounters"][3]
        if not c["resultsEqual"] or not c["resultsEqualRun2"] or not c["resultsEqualReusedDataset"]:
            tot["unequal"] += 1
        if bail_on:
            tot["bail_control" if c["control"] else "bail_on_noncontrol"] += 1
        if not c["planStable"] or not c["readStable"]:
            tot["unstable"].append((k["kind"], name))
        tot["scan_classes"].update(on["info"]["scanClasses"])
        if on["foreignLeavesRead"] and name not in ("1e", "1f", "1j"):
            tot["foreign_read_on"].append((k["kind"], name, on["foreignLeavesRead"]))
        pruned = on["info"]["reads"] != off["info"]["reads"]
        if pruned:
            tot["pruned"] += 1
        if not c["control"] and not pruned:
            tot["should_prune_but_not"].append((k["kind"], name))
        if not c["control"] and on["bytesRead"] >= off["bytesRead"]:
            tot["bytes_not_lower"].append((k["kind"], name, off["bytesRead"], on["bytesRead"]))
        if verbose or bail_on or not c["resultsEqual"]:
            print(f"  {name:3} eq={c['resultsEqual']} stable={c['planStable']} bail={bail_on} "
                  f"bytes {off['bytesRead']}->{on['bytesRead']} reads={on['info']['reads'][0][:90]}")
print("\nTOTALS", json.dumps({k: (sorted(v) if isinstance(v, set) else v) for k, v in tot.items()},
                             default=str, indent=1))
