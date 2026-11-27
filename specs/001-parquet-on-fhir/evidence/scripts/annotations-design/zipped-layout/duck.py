"""DuckDB reads on the zipped and parallel layouts: plan projections and bytes actually read.

Usage: duck.py <data root>. Bytes are counted by serving the Parquet file through an fsspec
filesystem that records every read range (DuckDB's own tooling shows the projected columns but,
in this version, no byte count). Prints ROW lines: variant, layout, case, bytes read (sum of the
distinct byte ranges, so a re-read of the footer is not double counted), and the scan's
Projections line from EXPLAIN.
"""
import json
import sys

import duckdb
import fsspec
from fsspec.implementations.local import LocalFileSystem

ROOT = sys.argv[1]


class CountingFS(LocalFileSystem):
    protocol = "counting"
    ranges = []

    def _open(self, path, mode="rb", **kw):
        f = super()._open(path, mode, **kw)
        orig = f.read
        pos = f.seek

        def read(n=-1):
            start = f.tell()
            data = orig(n)
            CountingFS.ranges.append((start, start + len(data)))
            return data

        f.read = read
        return f


def merged(rs):
    total, end = 0, -1
    for s, e in sorted(rs):
        if e <= end:
            continue
        total += e - max(s, end)
        end = e
    return total


def scan_projections(node):
    """The Projections of the Parquet scan node, found anywhere in the EXPLAIN JSON tree."""
    for n in node if isinstance(node, list) else [node]:
        info = n.get("extra_info", {})
        if isinstance(info, dict) and info.get("Function") == "READ_PARQUET":
            return json.dumps(info.get("Projections"))
        found = scan_projections(n.get("children", []))
        if found:
            return found
    return None


fsspec.register_implementation("counting", CountingFS, clobber=True)

CASES = [
    ("a1-bd", "p", "select birthDate from t"),
    ("a1-bd", "z", "select birthDate.value from t"),
    ("a1-bd-whole", "z", "select birthDate from t"),
    ("a2-family", "p", "select list_transform(name, lambda n: n.family) from t"),
    ("a2-family", "z", "select list_transform(name, lambda n: n.family.value) from t"),
    ("a2-family-dot", "p", "select unnest(name).family from t"),
    ("a2-family-dot", "z", "select unnest(name).family.value from t"),
    ("a3-given", "p", "select list_transform(name, lambda n: n.given) from t"),
    ("a3-given", "z", "select list_transform(name, lambda n: list_transform(n.given, lambda g: g.value)) from t"),
    ("a3-given-dot", "p", "select unnest(n.given) from (select unnest(name) n from t)"),
    ("a3-given-dot", "z", "select unnest(n.given).value from (select unnest(name) n from t)"),
    ("a4-event", "p", "select list_transform(dosage, lambda d: d.timing.event) from t"),
    ("a4-event", "z", "select list_transform(dosage, lambda d: list_transform(d.timing.event, lambda e: e.value)) from t"),
    ("a4-event-dot", "p", "select unnest(d.timing.event) from (select unnest(dosage) d from t)"),
    ("a4-event-dot", "z", "select unnest(d.timing.event).value from (select unnest(dosage) d from t)"),
    ("c1-start", "p", "select list_transform(dosage, lambda d: list_transform(d.timing._event, lambda m: m.__start)) from t"),
    ("c1-start", "z", "select list_transform(dosage, lambda d: list_transform(d.timing.event, lambda e: e.__start)) from t"),
    ("c2-start-dot", "p", "select unnest(d.timing._event).__start from (select unnest(dosage) d from t)"),
    ("c2-start-dot", "z", "select unnest(d.timing.event).__start from (select unnest(dosage) d from t)"),
]

for variant in ("m0", "m1"):
    for layout, d in (("p", "parallel"), ("z", "zipped")):
        path = f"{ROOT}/{variant}/{d}/part.parquet"
        for cid, lay, sql in CASES:
            if lay != layout:
                continue
            for mode in ("fs", "plain"):
                con = duckdb.connect()
                if mode == "fs":
                    con.register_filesystem(CountingFS())
                    # The fsspec adapter seeks and reads in two calls, so one thread keeps the counts exact.
                    con.execute("set threads=1")
                    src = f"counting://{path}"
                else:
                    src = path
                con.execute(f"create view t as select * from read_parquet('{src}')")
                label = f"{variant} {lay} {cid}"
                try:
                    if mode == "fs":
                        # An aggregate over a hash forces a full evaluation without shipping rows.
                        # The fsspec adapter is occasionally flaky, so a failed run is retried.
                        for attempt in range(5):
                            CountingFS.ranges.clear()
                            try:
                                con.execute(f"select count(*), sum(hash(x)) from ({sql}) q(x)").fetchall()
                                break
                            except Exception:
                                if attempt == 4:
                                    raise
                        print(f"ROW|{label}|bytes={merged(CountingFS.ranges)}", flush=True)
                    else:
                        plan = json.loads(con.execute(f"explain (format json) {sql}").fetchall()[0][1])
                        print(f"PLAN|{label}|{scan_projections(plan)}", flush=True)
                except Exception as e:
                    print(f"ROW|{label}|{mode}|ERROR|{str(e).splitlines()[0][:200]}", flush=True)
