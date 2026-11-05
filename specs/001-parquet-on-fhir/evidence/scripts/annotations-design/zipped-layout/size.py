"""File sizes and per-column chunk sizes from the Parquet footers, and a Polars projection check.

Usage: size.py <data root>.
"""
import os
import sys
from collections import defaultdict

import polars as pl
import pyarrow.parquet as pq

ROOT = sys.argv[1]
LEAVES = {
    "birthDate value": {"parallel": "birthDate", "zipped": "birthDate.value"},
    "name[].given[] value": {"parallel": "name.list.element.given.list.element",
                             "zipped": "name.list.element.given.list.element.value"},
    "event[] value": {"parallel": "dosage.list.element.timing.event.list.element",
                      "zipped": "dosage.list.element.timing.event.list.element.value"},
    "event[] __start": {"parallel": "dosage.list.element.timing._event.list.element.__start",
                        "zipped": "dosage.list.element.timing.event.list.element.__start"},
    "gender value": {"parallel": "gender", "zipped": "gender.value"},
}
for variant in ("m0", "m1"):
    sizes = {}
    chunks = {}
    for layout in ("parallel", "zipped"):
        path = f"{ROOT}/{variant}/{layout}/part.parquet"
        sizes[layout] = os.path.getsize(path)
        md = pq.ParquetFile(path).metadata
        per = defaultdict(lambda: [0, 0])
        for rg in range(md.num_row_groups):
            for c in range(md.num_columns):
                col = md.row_group(rg).column(c)
                per[col.path_in_schema][0] += col.total_compressed_size
                per[col.path_in_schema][1] += col.total_uncompressed_size
        chunks[layout] = per
        print(f"SIZE|{variant}|{layout}|file={sizes[layout]}|rowgroups={md.num_row_groups}|"
              f"leaves={md.num_columns}|compressed={sum(v[0] for v in per.values())}|"
              f"uncompressed={sum(v[1] for v in per.values())}")
    for name, paths in LEAVES.items():
        p = chunks["parallel"][paths["parallel"]]
        z = chunks["zipped"][paths["zipped"]]
        print(f"LEAF|{variant}|{name}|parallel={p[0]}/{p[1]}|zipped={z[0]}/{z[1]}")
    for layout in ("parallel", "zipped"):
        top = defaultdict(int)
        for k, v in chunks[layout].items():
            top[k.split(".")[0]] += v[0]
        print(f"TOP|{variant}|{layout}|" + "|".join(f"{k}={v}" for k, v in top.items()))
        # The leaves that carry only metadata and are null everywhere in M0.
        for k, v in sorted(chunks[layout].items()):
            if variant == "m0" and (k.endswith(".id") or ".extension." in k or "._" in k or k.startswith("_")):
                print(f"META|{variant}|{layout}|{k}|{v[0]}/{v[1]}")

# Polars: does the projection of a struct field reach the reader? Plan text only.
for layout, expr in (("parallel", pl.col("birthDate")),
                     ("zipped", pl.col("birthDate").struct.field("value"))):
    lf = pl.scan_parquet(f"{ROOT}/m1/{layout}/part.parquet").select(expr)
    plan = lf.explain().replace("\n", " ")
    print(f"POLARS|{layout}|birthDate|{' '.join(plan.split())[:300]}")
lf = pl.scan_parquet(f"{ROOT}/m1/zipped/part.parquet").select(
    pl.col("name").list.eval(pl.element().struct.field("given")).alias("g"))
print(f"POLARS|zipped|name-given-structlist|{' '.join(lf.explain().split())[:300]}")
