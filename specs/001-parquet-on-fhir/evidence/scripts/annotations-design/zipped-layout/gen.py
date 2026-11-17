"""Generates the same logical FHIR-like data in the PARALLEL and ZIPPED layouts, for M0 and M1.

Usage: gen.py <out dir> [rows]. Writes <out>/{m0,m1}/{parallel,zipped}/part.parquet, one writer
(pyarrow), snappy, 50,000-row row groups, for both layouts.
"""
import os
import random
import sys
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

OUT = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 200_000
TS = pa.timestamp("us", tz="UTC")
EXT = pa.list_(pa.struct([("url", pa.string()), ("valueString", pa.string())]))
GIVEN = ["Ann", "Bob", "Cara", "Dev", "Eve", "Finn", "Gus", "Hana", "Ivo", "Jun"]
FAMILY = ["Smith", "Jones", "Brown", "Lee", "Khan", "Nguyen", "Garcia", "Muller"]


def meta_fields(date):
    f = [("id", pa.string()), ("extension", EXT)]
    return f + ([("__start", TS), ("__end", TS)] if date else [])


def meta_t(date):
    return pa.struct(meta_fields(date))


def zip_t(date):
    return pa.struct([("value", pa.string())] + meta_fields(date))


def schema(layout):
    if layout == "parallel":
        return pa.schema([
            ("id", pa.string()),
            ("birthDate", pa.string()), ("_birthDate", meta_t(True)),
            ("gender", pa.string()), ("_gender", meta_t(False)),
            ("name", pa.list_(pa.struct([
                ("family", pa.string()), ("_family", meta_t(False)),
                ("given", pa.list_(pa.string())), ("_given", pa.list_(meta_t(False)))]))),
            ("dosage", pa.list_(pa.struct([("timing", pa.struct([
                ("event", pa.list_(pa.string())), ("_event", pa.list_(meta_t(True)))]))]))),
        ])
    return pa.schema([
        ("id", pa.string()),
        ("birthDate", zip_t(True)), ("gender", zip_t(False)),
        ("name", pa.list_(pa.struct([
            ("family", zip_t(False)), ("given", pa.list_(zip_t(False)))]))),
        ("dosage", pa.list_(pa.struct([("timing", pa.struct([
            ("event", pa.list_(zip_t(True)))]))]))),
    ])


def ts(rng):
    return datetime(2000 + rng.randrange(25), 1 + rng.randrange(12), 1 + rng.randrange(28),
                    tzinfo=timezone.utc)


def date_str(rng):
    return ts(rng).strftime("%Y-%m-%d")


def ext(rng, n):
    return [{"url": f"http://example.org/ext/{k}",
             "valueString": "".join(rng.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=24))}
            for k in range(n)]


class Prim:
    """One primitive value with its metadata (None when it has none) and annotations."""

    def __init__(self, value, rng, heavy, date, seq):
        self.value = value
        self.start = self.end = None
        if date:
            self.start = ts(rng)
            self.end = ts(rng)
        self.id = self.ext = None
        if heavy and rng.random() < 0.5:
            self.id = f"el-{seq}"
            self.ext = ext(rng, rng.randrange(3, 6))
        self.date = date

    def has_meta(self):
        return self.id is not None or self.ext is not None or self.start is not None

    def meta(self):
        m = {"id": self.id, "extension": self.ext}
        if self.date:
            m.update({"__start": self.start, "__end": self.end})
        return m

    def zipped(self):
        return {"value": self.value, **self.meta()}


def par_list(prims):
    """Values and the aligned metadata list, which is null when no element has metadata."""
    vals = [p.value for p in prims]
    metas = [p.meta() if p.has_meta() else None for p in prims]
    return vals, (metas if any(p.has_meta() for p in prims) else None)


def logical(i, rng, heavy):
    seq = i * 100
    r = {"id": f"p{i}"}
    r["birthDate"] = Prim(date_str(rng), rng, heavy, True, seq)
    r["gender"] = Prim(rng.choice(["male", "female", "other"]), rng, False, False, seq)
    r["name"] = [
        {"family": Prim(rng.choice(FAMILY), rng, False, False, seq),
         "given": [Prim(rng.choice(GIVEN), rng, heavy, False, seq + 1 + k)
                   for k in range(rng.randrange(1, 4))]}
        for _ in range(rng.randrange(1, 4))]
    r["dosage"] = [
        {"event": [Prim(date_str(rng), rng, heavy, True, seq + 50 + k)
                   for k in range(rng.randrange(1, 4))]}
        for _ in range(rng.randrange(1, 3))]
    return r


def parallel(r):
    bd = r["birthDate"]
    g = r["gender"]
    names = []
    for n in r["name"]:
        gv, gm = par_list(n["given"])
        fam = n["family"]
        names.append({"family": fam.value, "_family": fam.meta() if fam.has_meta() else None,
                      "given": gv, "_given": gm})
    dos = []
    for d in r["dosage"]:
        ev, em = par_list(d["event"])
        dos.append({"timing": {"event": ev, "_event": em}})
    return {"id": r["id"], "birthDate": bd.value,
            "_birthDate": bd.meta() if bd.has_meta() else None,
            "gender": g.value, "_gender": g.meta() if g.has_meta() else None,
            "name": names, "dosage": dos}


def zipped(r):
    return {"id": r["id"], "birthDate": r["birthDate"].zipped(), "gender": r["gender"].zipped(),
            "name": [{"family": n["family"].zipped(), "given": [p.zipped() for p in n["given"]]}
                     for n in r["name"]],
            "dosage": [{"timing": {"event": [p.zipped() for p in d["event"]]}}
                       for d in r["dosage"]]}


for variant, heavy in (("m0", False), ("m1", True)):
    rng = random.Random(42)
    rows = [logical(i, rng, heavy) for i in range(N)]
    for layout, conv in (("parallel", parallel), ("zipped", zipped)):
        d = f"{OUT}/{variant}/{layout}"
        os.makedirs(d, exist_ok=True)
        tbl = pa.Table.from_pylist([conv(r) for r in rows], schema=schema(layout))
        pq.write_table(tbl, f"{d}/part.parquet", compression="snappy", row_group_size=50_000)
        print(variant, layout, os.path.getsize(f"{d}/part.parquet"), flush=True)
