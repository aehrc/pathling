"""Compare split-benchmark JMH results against the T003 baseline (T136).

Usage: python3 t136-compare.py BASELINE.json RUN.json [RUN2.json ...]

For every benchmark and parameter combination, prints the JMH mean and 99.9%
error of each file, the range of per-fork means (one per JMH fork, computed from
the raw sample histogram), and the ratio of each run's mean to the baseline's.
"""

import json
import sys


def key(entry: dict) -> tuple:
    """Return a stable key naming the benchmark method and its parameter."""
    method = entry["benchmark"].rsplit(".", 1)[-1]
    params = entry.get("params", {})
    param = params.get("resourceType") or params.get("view")
    return method, param


def fork_means(entry: dict) -> list:
    """Return the mean sample time of each fork, from the raw sample histogram."""
    means = []
    for fork in entry["primaryMetric"]["rawDataHistogram"]:
        total = 0.0
        count = 0
        for iteration in fork:
            for value, n in iteration:
                total += value * n
                count += n
        means.append(total / count)
    return means


def load(path: str) -> dict:
    """Load a JMH JSON result file into a dictionary keyed by benchmark and parameter."""
    with open(path, encoding="utf-8") as f:
        return {key(e): e for e in json.load(f)}


def summarise(entry: dict) -> str:
    """Format a result as mean, error and per-fork range."""
    pm = entry["primaryMetric"]
    forks = fork_means(entry)
    return (
        f"{pm['score']:9.1f} ± {pm['scoreError']:6.1f} "
        f"[{min(forks):7.1f}–{max(forks):7.1f}]"
    )


def main(paths: list) -> None:
    """Print the comparison table."""
    baseline = load(paths[0])
    runs = [load(p) for p in paths[1:]]
    order = ["encode", "decode", "viewPlanning", "viewExecution"]
    for k in sorted(baseline, key=lambda k: (order.index(k[0]), k[1])):
        cells = [summarise(baseline[k])]
        for run in runs:
            if k in run:
                ratio = (
                    run[k]["primaryMetric"]["score"]
                    / baseline[k]["primaryMetric"]["score"]
                )
                cells.append(f"{summarise(run[k])} x{ratio:4.2f}")
            else:
                cells.append("missing")
        print(f"{k[0]:14s} {k[1]:30s} " + " | ".join(cells))


if __name__ == "__main__":
    main(sys.argv[1:])
