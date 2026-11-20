"""Turns the probe logs into the evidence tables. Usage: make_tables.py <4.3 log> <4.0 log>.

The class and blocker columns are judgements made from the read schemas and the optimised plans
(see blockers.sh), keyed by a substring of the view label.
"""
import re
import sys

PERIOD = ("`period._start` is read: the traversal builds `struct(value, start, range, "
          "named_struct(__start, _start.__start, __end, _start.__end))` and only `.value` is used, "
          "which stock Spark cannot trim")
SIZE = ("`size(filter(name, x -> x is not null))`: a presence-only use demands the whole element")
JUDGED = [
    ("`name.period.start`", "B", PERIOD),
    ("forEach name / `period.start`", "B", PERIOD),
    ("`address.period.start`", "B", PERIOD + " (address.period holds only start and _start here)"),
    ("`name.exists()`", "C", SIZE),
    ("`name.count()`", "C", SIZE),
    ("multi-column view", "B", "name read whole: the where is `size(filter(name, ...))` in a Filter"
     " (as " + "`name.exists()`" + "); address is pruned"),
    ("annotation-free leaf (use)", "C", "where is `size(filter(name, ...))` in a Filter"),
    ("exists(criteria)", "C", "where is `size(filter(name, ...))` in a Filter"),
]


def rows(path):
    out = []
    for line in open(path):
        if not line.startswith("| ") or line.startswith("| View") or line.startswith("| ---"):
            continue
        cells = re.split(r"(?<!\\) \| ", line.strip().strip("|").strip())
        out.append([c.strip() for c in cells])
    return out


def judge(label, auto, off, rw, on):
    for key, cls, blocker in JUDGED:
        if key in label:
            return cls, blocker
    if label.startswith("singular-meta"):
        return "A", "nothing to prune: the read is the same in all three"
    return auto, ""


def main():
    r43, r40 = rows(sys.argv[1]), rows(sys.argv[2])
    print("### Spark 4.3.0\n")
    print("| View | Original, rule off | Rewritten, rule off | Original, rule on | Class | Blocker "
          "| Results equal |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    counts = {}
    for c in r43:
        label, off, rw, on, auto, _, eq = c[0], c[1], c[2], c[3], c[4], c[5], c[6]
        cls, blocker = judge(label, auto, off, rw, on)
        counts.setdefault(cls, []).append(label)
        print(f"| {label} | {off} | {rw} | {on} | {cls} | {blocker} | {eq} |")
    print()
    for k in sorted(counts):
        print(f"- {k}: {len(counts[k])}")
    print("\n### Spark 4.0.2\n")
    print("| View | Original, rule off | Rewritten, rule off | Original, rule on | Results equal |")
    print("| --- | --- | --- | --- | --- |")
    for c in r40:
        print(f"| {c[0]} | {c[1]} | {c[2]} | {c[3]} | {c[6]} |")


main()
