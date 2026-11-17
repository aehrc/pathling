"""Turns the probe logs into the evidence tables. Usage: make_tables.py <4.3 log> <4.0 log>.

Classes are computed by the probe from the leaves Parquet reads: A when the arm reads a subset of
what the lambda-pruning rule reads (equal or better), C when it reads what the original reads,
otherwise B. Nothing is judged by hand.
"""
import re
import sys
from collections import Counter

NAMES = ["O", "P", "R", "PR", "PRD", "D", "eqP", "eqR", "eqPR", "eqPRD", "eqD", "rows", "applied",
         "cP", "cR", "cPR", "cPRD", "composes", "strict", "kind"]


def load(path):
    rows, diffs = [], []
    for line in open(path):
        parts = line.rstrip("\n").split("\t")
        if parts[0] == "ROW":
            d = dict(zip(NAMES, parts[2:]))
            d["label"] = parts[1]
            rows.append(d)
        elif parts[0] == "DIFF":
            diffs.append(parts[1:])
        elif parts[0] == "ERROR":
            print("ERROR in", path, parts[1:], file=sys.stderr)
    return rows, diffs


def esc(s):
    return s.replace("|", "\\|") if "\\|" not in s else s


def equal(d):
    ok = all(d[k] == "yes" for k in ("eqP", "eqR", "eqPR", "eqPRD", "eqD"))
    return ("yes" if ok else "**NO**") + f" ({d['rows']} rows)"


def table(rows, ver):
    print("| View | O | D3b | P | P+R | P+R+D3b | P / P+R / P+R+D3b | Rewrites applied | Results equal |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for d in rows:
        print(f"| {d['label']} | {d['O']} | {d['D']} | {d['P']} | {d['PR']} | {d['PRD']} | "
              f"{d['cP']} / {d['cPR']} / {d['cPRD']} | {d['applied']} | {equal(d)} |")
    print()


def counts(rows, title):
    print(f"{title} ({len(rows)} views):\n")
    print("| Plan | A | B | C |")
    print("| --- | --- | --- | --- |")
    for name, key in (("P (compute-first only)", "cP"), ("R (rewrites only)", "cR"),
                      ("P+R", "cPR"), ("P+R+D3b", "cPRD")):
        c = Counter(d[key] for d in rows)
        print(f"| {name} | {c['A']} | {c['B']} | {c['C']} |")
    print()
    comp = [d["label"] for d in rows if d["composes"] != "true"]
    print(f"- P+R+D3b reads no more than either P+R or D3b alone: "
          f"{len(rows) - len(comp)} of {len(rows)}." + (f" Not: {', '.join(comp)}." if comp else ""))
    bad = [d["label"] for d in rows if equal(d).startswith("**")]
    print(f"- Results equal to O in every arm: {len(rows) - len(bad)} of {len(rows)}."
          + (f" Not: {', '.join(bad)}." if bad else ""))
    notA = [d["label"] for d in rows if d["cPR"] != "A"]
    shown = "none" if not notA else (", ".join(notA) if len(notA) <= 6 else f"{len(notA)} views")
    print(f"- P+R not A: {shown}.")
    print()


def semantic(path):
    """Prints the SemanticProbe table: patients differing from the engine, per candidate form."""
    cells, order, examples = {}, [], []
    for line in open(path):
        parts = line.rstrip("\n").split("\t")
        if parts[0] != "SEM":
            continue
        fn, crit, tag, res = parts[1:5]
        kind = tag.split(":")[0].replace("native, coalesced", "native + coalesce")
        m = re.match(r"differing=(\d+/\d+)(.*)", res)
        key = (fn, crit)
        if key not in order:
            order.append(key)
        cells.setdefault(key, {})[kind] = m.group(1)
        if m.group(2) and kind in ("pure", "final", "native + coalesce"):
            examples.append(f"{fn}({crit}), {kind}:{m.group(2)}")
    kinds = ["pure", "native", "native + coalesce", "final"]
    print("| Function | Criteria | " + " | ".join(kinds) + " |")
    print("| --- | --- | " + " | ".join("---" for _ in kinds) + " |")
    for key in order:
        print(f"| {key[0]} | `{key[1]}` | " + " | ".join(cells[key][k] for k in kinds) + " |")
    print("\nExamples of the patients that differ, for the pure and coalesced forms:\n")
    for e in examples:
        print("- " + e)
    print()


def main():
    if len(sys.argv) > 3:
        semantic(sys.argv[3])
        return
    for ver, path in (("4.3.0", sys.argv[1]), ("4.0.2", sys.argv[2])):
        rows, diffs = load(path)
        main26 = [d for d in rows if d["kind"] == "base" and not d["label"].startswith("edge:")]
        extra = [d for d in rows if d["kind"] == "extra" and not d["label"].startswith("edge:")]
        edge = [d for d in rows if d["label"].startswith("edge:")]
        print(f"### Spark {ver}\n")
        counts(main26, "The views of the previous spike")
        table(main26, ver)
        counts(extra, "Extra views, for the existence rewrites")
        table(extra, ver)
        counts(edge, "Edge data set (null elements in name and address), all views")
        table(edge, ver)
        if diffs:
            print("Differing rows:\n")
            for dd in diffs:
                print("- " + " | ".join(dd))
            print()


main()
