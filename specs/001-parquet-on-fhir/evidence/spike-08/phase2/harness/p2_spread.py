"""Spike 08 phase 2: the replicate spread between two labels of the same build (the A0 noise check).

For every source, type and phase, the ratio of the two labels' pooled minimums (ten samples each: the
extensions-off and extensions-on runs, which are the same code). Items whose pooled minimum is under 1 s are
listed but not judged, because fixed per-job costs dominate them.
Exit status 0 where the spread is acceptable, 1 where it is not:
  acceptable  every judged ratio within 0.85 to 1.15, and their median distance from 1 at most 0.05.
Usage: p2_spread.py <label-1> <label-2>
"""
import glob, json, os, statistics, sys

HERE = os.path.dirname(os.path.abspath(__file__))
a_label, b_label = sys.argv[1:]


def load(label):
    out = {}
    for f in glob.glob(f"{HERE}/results/{label}/phases-*.json"):
        d = json.load(open(f))
        for t, r in d["results"].items():
            for key in ("fitMs", "encodeMs", "decodeMs"):
                out.setdefault((d["source"], t, key), []).extend(r[key]["samples"])
    return {k: min(v) for k, v in out.items()}


a, b = load(a_label), load(b_label)
judged, rows = [], []
for k in sorted(set(a) & set(b)):
    r = b[k] / a[k]
    big = min(a[k], b[k]) >= 1000
    rows.append(f"{k[0]:16} {k[1]:12} {k[2][:-2]:7} {a[k]:9,.0f} {b[k]:9,.0f} {r:5.2f}{'' if big else '  (under 1 s, not judged)'}")
    if big:
        judged.append(abs(r - 1))
print(f"{'source':16} {'type':12} {'phase':7} {a_label[:9]:>9} {b_label[:9]:>9} ratio")
print("\n".join(rows))
if not judged:
    print("nothing to judge")
    sys.exit(1)
worst, median = max(judged), statistics.median(judged)
ok = worst <= 0.15 and median <= 0.05
print(f"\njudged items {len(judged)}; worst |ratio - 1| {worst:.3f}; median {median:.3f}; "
      f"{'ACCEPTABLE' if ok else 'TOO NOISY'} (limits 0.15 and 0.05)")
sys.exit(0 if ok else 1)
