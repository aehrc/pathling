# What unused element metadata costs when the source is in memory

Measured for requirement C4-compute of `design/annotations-in-the-engine.md` (sections 2 to 4), in
answer to the engine owner's question: "Not sure how important the in-memory 'pruning' is. It's
definitely required for lazy computation, but is decoding of extra fields a noticeable performance
factor?"

**Verdict: in memory, decoding unused metadata is not a noticeable CPU cost, and layer 1 does not
change that; what is noticeable is the cache memory, which no planner shaping can reduce.**

- **CPU.** With 9.7 times (Patient) and 14.2 times (Encounter) as many cached bytes, the unused
  metadata moved the median of a noop write over 1M Patients (or 500k Encounters) by -0.01 to
  +0.08 s on every arm that does not pair. That is at most about 0.1 microsecond per row, against
  a run-to-run spread of about 0.03 s, and the slowest arm takes 0.26 s in total. The percentages
  in the table (up to +90 percent) are large only because the bases are 0.03 to 0.2 s.
- **Why decoding is free.** Spark 4.0.2 stores a nested column (struct or array) in the cache as
  one opaque `UnsafeRow` or `UnsafeArrayData` blob per row, and reading it points into the buffer
  without copying or parsing (`ColumnType.scala`, verified below). Nested content that no
  expression touches is never visited. Only top-level columns are pruned.
- **Pairing, not decoding, is the avoidable computation.** Pairing `x` with its whole `_x` at
  traversal (A1) costs 0.23 s extra per 1M Patients on `name.given` even on data with no
  metadata (0.33 s against 0.10 s for layer 1), because it builds a zipped array of structs per
  name. That cost is independent of the payload. Layer 1 removes it. It does not measurably
  remove the small payload-dependent part (+0.02 to +0.08 s), which every arm including a bare
  read pays.
- **Memory.** The cached Patient table is 4,191 MB with metadata and 432 MB without; Encounter is
  2,798 MB and 197 MB. Layer 1 cannot touch this, because the cache holds the whole relation.
  Only an engine that caches a projection of the relation, or a source that is not cached, avoids
  it.

C4-compute is met by layer 1 on every in-memory source for the arms measured, in the sense that
the pairing is gone and the ext/plain gap is within noise. The measurement does not show that
pruning matters for CPU in memory. It matters for pairing (computation) and for bytes read from
Parquet (not measured here; see the earlier evidence).

## Conditions

| | |
| --- | --- |
| Commit | `0c94065ec3` plus the working tree, branch `issue/2832_41-42_decimal-and-annotations`; the jars as built there |
| Spark | 4.0.2, `local[4]`, `-Xmx12g`, adaptive execution off, default cache serializer and storage level |
| Machine | 11 logical cores; every timed JVM ran under `lockf` on the shared benchmark lock |
| Method | `write.format("noop")`, median of five after one warm-up, one JVM per source, variant and arm; `System.gc()` before each run (excluded from the time) |
| Data | 1,000,000 Patients (mean 2.5 names, 1 to 3 givens each) and 500,000 Encounters (mean 2 locations, a `period.start` each, 2010 to 2025, some at partial precision) |
| `ext` | Five string extensions of 128 to 199 hex characters each (random, so incompressible) on `_birthDate`, on each `name`, on about one `_given` in ten, on each `location`, each `period` and each `period._start`. About 18 payloads per Patient and 30 per Encounter |
| `plain` | The same resources and schema. Every metadata field is present and null, so ext differs from plain in content only |
| Cache | `df.cache()`, forced with a noop write and a `count()`; 4 partitions in every case (Parquet is repartitioned to 4 before caching); 0 MB spilled to disk in every case |

**S-cache** reads Parquet (written once by `Prep.java`) and caches it. **S-json** reads NDJSON
through `ptc.read().ndjson(...).read(type)` (the library's encoder), splices the metadata in and
caches it. The two sources end with the same cached content, so the cache sizes are identical and
S-json is a second run of the same measurement rather than an independent one. See the caveats for
why the metadata is spliced.

### Arms

| Arm | What it is |
| --- | --- |
| **bare** | Floor. Reads the values with a lambda over the parent column, nothing else (E3 uses the stored `__start` for the filter). |
| **A0** | **Engine code.** `ptc.read().datasets().dataset(type, cached).view(type).json(...)`, a ViewDefinition column with the FHIRPath expression, over the cached dataset. The stored `__start` and `__end` are read; no `date_range` is in the plan. |
| **A1** | Hand-built, E1 and E2. The design as written: `struct(value, meta)` with `meta` the whole `_x`, built inside the same lambda wrapper the engine plan has (`element_at(transform(filter(array(CASE ... struct(...)))))` for E1, `arrays_zip(given, _given)` per name for E2). |
| **L1** | Hand-built layer 1. Same wrapper as A1 with no `meta` (E1); `given` read without `_given` (E2); for E3 the engine's traversal with the element reduced to `value`, `__start`, `__end`. The parent lambda still carries whole `location` structs. |
| **D3a** | Hand-built, E3 only. The D3a-complex shape: `location.period.start`, `.._start.__start` and `.._start.__end` as extraction chains outside any lambda, `arrays_zip`, then the filter. |

The expressions are E1 `Patient.birthDate`, E2 `Patient.name.given` and E3
`Encounter.location.period.where(start > @2020-01-01).start`. The E3 comparison is the engine's own
range comparison against the literal range of `@2020-01-01`, copied from its plan.

All arms return the same results: E1 1,000,000 values; E2 4,146,033 givens (A0, A1, L1; `bare`
counts names, 2,073,240, by construction); E3 296,054 starts. Plain and ext agree.

## Results

Median seconds (minimum to maximum of the five runs) for the noop write, plain against ext, and the
difference. Times are small, and run-to-run spread is 0.01 to 0.05 s, so differences under about
0.03 s are not resolved.

| source | expr | arm | plain | ext | ext - plain (s) | (ext - plain)/plain |
| --- | --- | --- | --- | --- | --- | --- |
| cache | E1 | bare | 0.04 (0.03-0.04) | 0.03 (0.03-0.05) | -0.01 | -25% |
| cache | E1 | A0 | 0.04 (0.04-0.05) | 0.03 (0.03-0.04) | -0.01 | -25% |
| cache | E1 | A1 | 0.09 (0.08-0.09) | 0.09 (0.09-0.11) | +0.00 | +0% |
| cache | E1 | L1 | 0.08 (0.08-0.09) | 0.09 (0.09-0.10) | +0.01 | +12% |
| cache | E2 | bare | 0.06 (0.06-0.07) | 0.08 (0.08-0.08) | +0.02 | +33% |
| cache | E2 | A0 | 0.20 (0.19-0.21) | 0.20 (0.19-0.26) | +0.00 | +0% |
| cache | E2 | A1 | 0.33 (0.31-0.35) | 0.64 (0.26-0.77) | +0.31 | +94% |
| cache | E2 | L1 | 0.10 (0.09-0.10) | 0.12 (0.11-0.12) | +0.02 | +20% |
| cache | E3 | bare | 0.05 (0.05-0.05) | 0.07 (0.07-0.08) | +0.02 | +40% |
| cache | E3 | A0 | 0.18 (0.17-0.18) | 0.22 (0.20-0.24) | +0.04 | +22% |
| cache | E3 | L1 | 0.10 (0.10-0.12) | 0.16 (0.13-0.17) | +0.06 | +60% |
| cache | E3 | D3a | 0.11 (0.11-0.12) | 0.14 (0.13-0.15) | +0.03 | +27% |
| json | E1 | bare | 0.04 (0.03-0.04) | 0.05 (0.04-0.06) | +0.01 | +25% |
| json | E1 | A0 | 0.04 (0.04-0.05) | 0.06 (0.05-0.07) | +0.02 | +50% |
| json | E1 | A1 | 0.09 (0.08-0.10) | 0.11 (0.10-0.12) | +0.02 | +22% |
| json | E1 | L1 | 0.07 (0.07-0.07) | 0.10 (0.09-0.10) | +0.03 | +43% |
| json | E2 | bare | 0.07 (0.07-0.08) | 0.13 (0.11-0.14) | +0.06 | +86% |
| json | E2 | A0 | 0.23 (0.21-0.24) | 0.25 (0.23-0.28) | +0.02 | +9% |
| json | E2 | A1 | 0.34 (0.30-0.40) | 0.44 (0.34-0.57) | +0.10 | +29% |
| json | E2 | L1 | 0.09 (0.09-0.11) | 0.17 (0.15-0.19) | +0.08 | +89% |
| json | E3 | bare | 0.06 (0.06-0.08) | 0.09 (0.08-0.09) | +0.03 | +50% |
| json | E3 | A0 | 0.19 (0.18-0.20) | 0.26 (0.23-0.32) | +0.07 | +37% |
| json | E3 | L1 | 0.12 (0.11-0.13) | 0.14 (0.14-0.18) | +0.02 | +17% |
| json | E3 | D3a | 0.12 (0.12-0.14) | 0.14 (0.14-0.15) | +0.02 | +17% |

Cache size, from `sparkContext.getRDDStorageInfo` (all in memory, none on disk):

| Table | Rows | plain | ext | ratio |
| --- | --- | --- | --- | --- |
| Patient | 1,000,000 | 432 MB | 4,191 MB | 9.7x |
| Encounter | 500,000 | 197 MB | 2,798 MB | 14.2x |

Time to build the cache (not part of the timed runs): Patient ext 3.2 s from Parquet and 11.8 s
from NDJSON including the encode and splice; plain 1.5 s and 3.1 s.

### What the numbers support

1. **The ext/plain gap of every non-pairing arm is 0 to 0.08 s**, in both sources. The arms with
   the smallest bases show the largest percentages (bare `name.given` +86 percent of 0.07 s).
   A0, L1 and D3a on E3 differ from each other by no more than the spread, so layer 1 and D3a
   cannot be told apart from today's engine on the unused metadata in memory.
2. **A0, the engine today, already does not carry unused metadata for these three expressions.**
   Over a dataset in the Parquet on FHIR shape, E1 scans only `birthDate`, and E2 and E3 build
   their element from `given` and from `start` and the stored range alone. It is slower than L1
   by 0.1 s on E2 and E3 in plain as well as ext, which is fixed per-row engine overhead
   (`Generate`, wrappers), not metadata.
3. **A1 pays for pairing.** On E2, A1 is 0.23 to 0.25 s above L1 on plain data. That is the
   cost of building the paired array per name when no extension exists. The ext gap of A1 on E2
   is +0.10 s (json) and +0.31 s (cache), but the cache figure's five runs span 0.26 to 0.77 s
   and it is not reliable. On E1 A1 is 0.01 to 0.02 s above L1, the same on ext, since `_birthDate`
   is a separate top-level column and costs only its scan.
4. **Bare and L1 on E2 and E3 still show +0.02 to +0.08 s.** Those arms read the whole `name` or
   `location` blob and touch more memory per row when it is large. That is a bandwidth and
   cache-miss effect of a 10 to 14 times larger cached relation, and layer 1 cannot change it.

## Why: how the in-memory cache stores nested columns

Read from the Spark 4.0.2 sources (`spark-sql_2.13-4.0.2-sources.jar`, copied to the spike
directory), and checked against the executed plans:

- `ColumnType.scala` defines `STRUCT`, `ARRAY` and `MAP` as `DirectCopyColumnType`s over
  `UnsafeRow`, `UnsafeArrayData` and `UnsafeMapData`. `append` writes the row's bytes with a length
  prefix: one opaque blob per row per top-level column. `extract` reads the length, then
  `pointTo`s a new `UnsafeRow` or `UnsafeArrayData` at the buffer's current cursor. It copies and
  parses nothing.
- So a nested column is read as a pointer into the batch, and an expression then reaches into it
  by offset. A field or a whole sub-array nobody dereferences is never touched. This is why
  decoding is not a cost.
- The cache has no nested-field pruning. `InMemoryTableScan` takes top-level attributes only. The
  executed plans show `InMemoryTableScan [name#2]` for E2 and `[location#4]` for E3 under every
  arm, including D3a, whose extraction chains would prune to leaves on a Parquet scan. They show
  `[birthDate#3]` for E1 under A0, bare and L1, and `[_birthDate#4, birthDate#3]` under A1:
  a separate `_x` column is pruned when nothing references it, a nested one is not.
- The size cost follows from the same layout: the blob holds every byte of the row, and the
  default cache compression (`spark.sql.inMemoryColumnarStorage.compressed`) does not apply to
  struct and array blobs.

## Caveats

- **Measured against inferred.** The timings, cache sizes, result counts and plans are measured.
  That the cost of the unused bytes is a pointer dereference and memory bandwidth, and not
  decoding, is inferred from the source and the near-zero gap; it was not profiled.
- **The metadata is spliced, not read.** On this branch the library's reader stores no `_x` group
  for a primitive (`ResourceTransformer` states they are null until M5), and the encoder drops
  them, so no NDJSON can produce `_birthDate.extension` or `_given`. `Splice.java` adds all
  extension payloads with SQL after encoding, in both variants, with the same schema. The NDJSON
  holds no extensions. S-json therefore tests the NDJSON-to-cache path, not payload decoding from
  NDJSON, and the two sources end with identical caches.
- **Resolution.** The timed runs last 0.03 to 0.35 s on 4 partitions. A spread of 0.03 s on the
  median is normal, so effects below that cannot be read. A larger table would give a better
  resolution, but would not fit the 12 GB heap with the ext variant cached.
- **Extra work outside the lambda is not measured.** Operators the engine adds for other
  expressions (joins of references, `repeat`, `unionAll`) were not run.
- **Not measured: a non-cached source.** Parquet reads prune nested leaves and the earlier
  evidence puts the bytes read at 10 to 14 times for carried metadata. This probe says nothing
  about that case, so it should not be read as removing it.
- **Payloads are incompressible and unique.** Real extensions repeat, so Parquet would compress
  them, but the in-memory cache would still hold them in full, so the cache ratio would be about
  the same.
- **The plain variant has the metadata columns, all null.** A schema without them would make the
  pairing arms impossible to write. The cost of carrying a null column is in both variants.
- **First run discarded.** The first pass of S-cache gave the ext table 23 cached partitions and
  the plain one 4, which confounds the time with parallelism; the source was re-run with both
  repartitioned to 4. The results here are the second pass.
- The A1 arm is hand-built and is a floor for a real implementation, as in the earlier probes.

## Reproduction

All in `scripts/annotations-design/cached-source/`, run from the worktree root. `env.sh` holds the
paths. The third-party classpath is generated on first use by `../common.sh`;
`THIRDPARTY_CP` overrides it.

```bash
./run.sh data                 # NDJSON, encode, splice, Parquet (N_PAT and N_ENC set the sizes)
./run.sh bench [results file] # every arm, under the benchmark lock (SOURCES=cache or json to limit)
python3 table.py results-1M.txt
./run.sh one <cache|json> <plain|ext> <bare|A0|A1|L1|D3a> <E1,E2|E3>   # a single JVM
```

`results-1M.txt` is the raw output (every RESULT line carries the median, minimum, maximum, GC
milliseconds, result count, and the `InMemoryTableScan` columns). `SchemaProbe.java` prints the
encoded schemas. The data is under
`.local/work/issue/2832_41-42_decimal-and-annotations/spikes/cached-source/`, which is ignored.
