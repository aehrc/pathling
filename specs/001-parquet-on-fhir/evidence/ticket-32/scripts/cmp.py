import json, os, sys
H="/Users/szu004/dev/pathling/.local/work/issue/2367/pathling-compare/results"
bar, mine = sys.argv[1], sys.argv[2]
def load(label, src, ext):
    p=f"{H}/{label}/phases-{src}-ext{ext}.json"
    if not os.path.exists(p): return None
    d=json.load(open(p)); return d.get("results", d)
print(f"{'source':16} {'type':12} {'phase':7} {'bar':>8} {'this':>8} {'ratio':>6}")
for src in ["json","xml-lines","xml-bundles","xml-bundles-txn"]:
    for t in ["Patient","Condition","Encounter","Observation"]:
        for ph in ["fitMs","encodeMs","decodeMs"]:
            b=[x for e in (0,1) if (r:=load(bar,src,e)) and t in r for x in r[t][ph]["samples"]]
            m=[x for e in (0,1) if (r:=load(mine,src,e)) and t in r for x in r[t][ph]["samples"]]
            if b and m: print(f"{src:16} {t:12} {ph[:-2]:7} {min(b):8.0f} {min(m):8.0f} {min(m)/min(b):6.2f}")
