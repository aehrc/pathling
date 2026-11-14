"""Compare the 2x2 benchmark matrix of #2879: Pathling 10.0.0 and 11, each on Spark 4.0.2 and 4.1.0.

Usage: python3 spark-4-1-compare.py RESULTS_DIR

RESULTS_DIR holds one JMH JSON file per cell, phase and pass, named
bench-{10,11}-spark{402,410}-{phase}-pass{1,2}.json. Each pass of each phase ran on one node, with
the four cells back to back. For every benchmark and parameter, prints a Markdown row with each
cell's mean over the two passes, then four ratios, each given as its range over the two passes
(pass 1 against pass 1, pass 2 against pass 2):

- 11 / 10 on Spark 4.0.2 and on Spark 4.1.0, the regression check;
- Spark 4.1.0 / Spark 4.0.2 for 10 and for 11, the effect of the Spark upgrade.

A final line gives the largest difference between the two passes of any cell. The passes ran on
different nodes, so this is the variation between runs that a ratio has to exceed to mean anything.

Author: John Grimes
"""

import json
import sys
from pathlib import Path

CELLS = ["10-spark402", "11-spark402", "10-spark410", "11-spark410"]
PASSES = ["pass1", "pass2"]
ORDER = ["encode", "decode", "viewPlanning", "viewExecution"]


def key(entry: dict) -> tuple:
    """Return a stable key naming the benchmark method and its parameter."""
    method = entry["benchmark"].rsplit(".", 1)[-1]
    params = entry.get("params", {})
    return method, params.get("resourceType") or params.get("view")


def load(results: Path, cell: str, run: str) -> dict:
    """Load the JMH JSON files of one cell and pass, across all phases, into mean scores by key."""
    scores = {}
    for path in sorted(results.glob(f"bench-{cell}-*-{run}.json")):
        with path.open(encoding="utf-8") as f:
            scores.update({key(e): e["primaryMetric"]["score"] for e in json.load(f)})
    return scores


def ratio_range(scores: dict, numerator: str, denominator: str, k: tuple) -> str:
    """Format the range over the passes of the ratio of two cells for one benchmark."""
    ratios = [scores[(numerator, p)][k] / scores[(denominator, p)][k] for p in PASSES]
    low, high = min(ratios), max(ratios)
    return f"{low:.2f}" if f"{low:.2f}" == f"{high:.2f}" else f"{low:.2f}–{high:.2f}"


def main(results: Path) -> None:
    """Print the comparison table."""
    scores = {(cell, p): load(results, cell, p) for cell in CELLS for p in PASSES}
    keys = sorted(
        scores[(CELLS[0], PASSES[0])], key=lambda k: (ORDER.index(k[0]), k[1])
    )
    print(
        "| Phase | Subject | 10 / 4.0.2 | 11 / 4.0.2 | 10 / 4.1.0 | 11 / 4.1.0 "
        "| 11:10 on 4.0.2 | 11:10 on 4.1.0 | 4.1:4.0 for 10 | 4.1:4.0 for 11 |"
    )
    print("| --- | --- |" + " ---: |" * 8)
    drift = 0.0
    for k in keys:
        means = []
        for cell in CELLS:
            a, b = (scores[(cell, p)][k] for p in PASSES)
            drift = max(drift, abs(a - b) / min(a, b))
            means.append(f"{(a + b) / 2:.1f}")
        ratios = [
            ratio_range(scores, "11-spark402", "10-spark402", k),
            ratio_range(scores, "11-spark410", "10-spark410", k),
            ratio_range(scores, "10-spark410", "10-spark402", k),
            ratio_range(scores, "11-spark410", "11-spark402", k),
        ]
        print(f"| {k[0]} | `{k[1]}` | " + " | ".join(means + ratios) + " |")
    print(f"\nLargest difference between the two passes of one cell: {drift:.1%}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
