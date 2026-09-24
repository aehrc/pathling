# The schema merge gate

T119, the Phase 3 gate. R-015 puts `mergeSchema` on by default on read, with an
opt-out, and records the condition on that decision: *measure merge cost over a
realistic file count on object storage; the result can force a fallback for raw
files.* Decision 29 repeats it. T118 enables the option, so the answer is wanted
before T118 is designed rather than after.

**There is no object-storage test harness in this repository.** No MinIO, no
testcontainers; the only `s3a://` string is a value in
`server/src/test/resources/application-unit-test.yml`, and `hadoop-aws` is
version-managed at the root `pom.xml` but wired into no test. Standing one up is
out of scope for a gate. This measurement is therefore **local, and Delta is
treated as the primary path** — it is the format Pathling's own sinks write, and
it stores its schema once in `_delta_log` rather than in N file footers. The gap
between what was measured and what R-015 asked for is stated in full below; it
is the most important part of this document, not a footnote.

**Result, in short.** For Delta the gate does not bite: schema resolution is
flat at 2–6 ms across the whole sweep, 185x to 370x below the raw merge at 4096
files. For raw Parquet the local data establishes the *shape* of the curve —
cost is linear in file count and clearly sublinear in schema width — and a
CPU-and-deserialisation floor of 0.06–0.09 ms per file narrow and 0.38 ms per
file wide. It does **not** establish the wall clock a user on S3 would see, and
it does not by itself force a fallback. What it does settle is a different
question that T118 has to answer anyway: **the opt-out must be schema *supply*,
not `mergeSchema=false`**, because `mergeSchema=false` was measured returning as
few as 6 of 24 leaf columns on a divergent corpus, silently. A supplied schema
is only safe while it covers the union of the files, though, and that union is
what a merge produces — so supply is a way to stop paying for the merge on every
read, not a way to never pay for it.

## Conditions

| | |
| --- | --- |
| Commit | `93c744be84` |
| Machine | Apple M3 Pro, 11 cores, 36 GB, macOS 26.6.2 |
| JVM | OpenJDK 21.0.3 |
| Spark | 4.0.2 (Scala 2.13), Delta 4.0.0 |
| Spark master | `local[*]`, `defaultParallelism` 11 |
| Storage | local APFS SSD, **page-cache warm** |
| Warm-up | 3 untimed iterations per cell |
| Measurement | 7 timed iterations per cell, **median** reported |
| Wall clock | 448.7 s for the whole spike |

The spike code is not in the repository. It was written into
`library-api/src/test/`, run with

    mvn test -pl library-api -Dtest=MergeCostSpike -DfailIfNoSpecifiedTests=false

and deleted. The transcript it produced is `merge-cost.txt`, beside this file.
`library-api` rather than `fhir-schema` because FR-048's `bannedDependencies`
rule forbids `spark-sql` and `spark-catalyst` in `fhir-schema`, test scope
included.

## What was measured

**Schema resolution at read, not query execution.** Each cell times
`spark.read(...).schema()` — the call that resolves the relation — and nothing
downstream of it. Four conditions per corpus:

| Condition | What it does |
| --- | --- |
| `mergeSchema=true` | Opens **every** file's footer, merges the results. The R-015 default. |
| `mergeSchema=false` | Opens **one** footer and assumes the rest match. |
| supplied schema | `spark.read().schema(derived).parquet(...)`. Lists files, opens **no** footers. This is the fallback R-015 contemplates. The schema supplied here is the full derived schema, which covers the union of the corpus by construction; see the gate section for why obtaining such a schema is not itself free. |
| Delta | `spark.read().format("delta").load(...)`, reading the schema from `_delta_log`. |

The corpora are synthetic files carrying a schema derived by `SchemaBuilder`
(T031) from the FHIR R4 definitions, one row per file:

- **narrow** — `SchemaBuilder.pruned("Patient", observed)`, 12 top-level fields,
  **24 leaf columns**. The pruned schema is the normal case.
- **wide** — `SchemaBuilder.dense("Observation")` at the default bounds, 55
  top-level fields, **1371 leaf columns**. The dense schema is the option.

**Divergent** corpora are written in eight appends whose schemas progressively
widen, so later files carry columns earlier ones lack — the situation R-015
exists for. A **uniform** corpus was run alongside the narrow divergent one to
separate divergence from file count.

File counts sweep 16 → 4096 geometrically. The upper half is the realistic part:
an append-per-batch warehouse writing daily for three years lands near 1000
files per resource type, and hourly for a year near 8760, before any
multiplication by the partition count of each write.

## Results

Median milliseconds to resolve the schema. Lower is better.

### Narrow (24 leaf columns), divergent

| Files | `mergeSchema=true` | `=false` | Supplied schema | Delta | Merge cost isolated |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 37.3 | 46.5 | 2.9 | 4.1 | 34.4 |
| 64 | 38.5 | 54.3 | 3.8 | 3.0 | 34.7 |
| 256 | 67.7 | 51.8 | 10.8 | 2.7 | 56.9 |
| 1024 | 187.6 | 86.8 | 39.6 | 2.8 | 148.0 |
| 4096 | 483.1 | 177.8 | 151.0 | 2.6 | 332.1 |

### Narrow (24 leaf columns), uniform

| Files | `mergeSchema=true` | `=false` | Supplied schema | Delta | Merge cost isolated |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 83.4 | 18.2 | 1.6 | 2.1 | 81.8 |
| 64 | 31.6 | 19.0 | 3.5 | 1.8 | 28.1 |
| 256 | 49.3 | 27.5 | 10.2 | 1.8 | 39.1 |
| 1024 | 169.8 | 116.8 | 37.7 | 2.0 | 132.1 |
| 4096 | 562.6 | 200.9 | 148.7 | 2.4 | 413.9 |

### Wide (1371 leaf columns), divergent

| Files | `mergeSchema=true` | `=false` | Supplied schema | Delta | Merge cost isolated |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 34.3 | 186.3 | 2.1 | 5.7 | 32.2 |
| 64 | 60.5 | 23.0 | 3.7 | 4.9 | 56.8 |
| 256 | 136.9 | 28.8 | 11.4 | 4.6 | 125.5 |
| 1024 | 388.1 | 88.6 | 37.9 | 4.4 | 350.2 |
| 4096 | 1658.9 | 173.0 | 151.0 | 4.5 | 1507.9 |

"Merge cost isolated" is `mergeSchema=true` minus the supplied-schema figure.
The supplied-schema column is file **listing** and relation construction with no
footer reads at all. The difference is therefore the footer work **plus the
launch of the Spark job that does it**, which the supplied-schema path never
pays — at 16 files that launch is most of the number. It is still the better
isolator than `mergeSchema=false`, for the reason in the next section, but the
per-file cost has to be read off the slope rather than off any single cell.

### The silent null-fill, measured

The width the reader actually returned, on the divergent corpora:

| Corpus | Files | `mergeSchema=true` | `mergeSchema=false` |
| --- | ---: | ---: | ---: |
| narrow | 16 / 64 / 256 / 1024 / 4096 | 24 every time | 10 / 6 / 6 / 15 / 20 |
| wide | 16 / 64 / 256 / 1024 / 4096 | 1371 every time | 39 / 415 / 162 / 859 / 162 |

Without merging the reader returns whichever single file Spark happened to touch
first, and every column the other files carry is dropped without a warning. The
figures are not even monotonic in file count, because which file wins is an
artefact of listing order rather than anything the user controls.

## What the numbers say

**Cost is linear in file count.** Across the last three points of every corpus
the isolated merge cost scales with N to within the noise: narrow divergent
56.9 → 148.0 → 332.1 for 256 → 1024 → 4096, and wide 125.5 → 350.2 → 1507.9.
The per-file cost is the **slope** between the 1024 and 4096 points, which
cancels the fixed job-launch cost:

| Corpus | Slope (ms per file) |
| --- | ---: |
| narrow divergent | 0.060 |
| narrow uniform | 0.092 |
| wide divergent | 0.377 |

This is the deserialisation-and-merge floor, with no I/O latency in it at all.
The intercepts implied by those two-point fits are +87, +38 and -36 ms across
the three corpora — inconsistent in sign, so they are the noise in these medians
rather than a resolvable fixed cost. All that should be taken from them is that
an overhead of the order of tens of milliseconds, the job launch, sits under
every cell.

**Cost rises with schema width, but far more slowly than width itself.** 1371
leaf columns are 57x the columns of 24, and cost about 4.5x as much to merge at
4096 files. Footer parsing is per column-chunk, but the per-file overhead is
large relative to it, so width matters considerably less than file count. No
functional form is fitted to two width points; the finding is the direction and
the order of magnitude, not a law.

**Divergence barely moves it.** At 1024 files the narrow divergent and uniform
corpora differ by 10% (148.0 vs 132.1), and at 4096 the uniform corpus is
actually the slower of the two. Every footer is read either way; divergence only
affects the reduce step, which is negligible. The value of divergence in this
experiment is the width column above, not the timings.

**`mergeSchema=false` is not a usable isolator and is noisy.** It launches the
same Spark job for a single file that the merge launches for N, so at small N
the job-launch cost dominates and swamps the difference — the wide 16-file cell
reads 186.3 ms against 34.3 ms for the full merge, which is a GC-and-JIT
artefact, not a result. Only above about 1000 files does the merge separate
cleanly from the machinery around it. Run-to-run variation of ±15% should be
assumed on every cell; the two narrow 4096 rows, which measure nearly the same
work, differ by 16%.

**Delta is flat.** 1.8–5.7 ms across the entire sweep, tracking schema width
slightly and file count not at all. At 4096 files it is 185x cheaper than the
number of files. Every table here carries eight commits and no checkpoint
(Delta checkpoints every tenth), so commit count was held fixed across the
sweep and **whether the figure grows with uncheckpointed commits was not
measured**. The small gap between the wide table's 4.5 ms and the narrow
table's 2.6 ms tracks schema width, not file count.

**The cost is paid per `DataSource` construction, and once per resource type.**
`FileSource.buildResourceMap` calls `reader.load(paths)` eagerly inside a
`Collectors.toMap` over every resource type it discovered, so constructing a
`ParquetSource` over a warehouse resolves the schema of *every* type present
before any query runs and whether or not that type is ever queried. The
per-resource-type figures above must therefore be multiplied by the number of
resource types in the warehouse to get the cost a user actually waits for.
Against `baseline.md`'s yardstick — a ~50 ms query planning floor and 20–450 ms
execution — a single narrow resource type at 4096 files (483 ms) already costs
about what the most expensive view in the benchmark suite costs to execute, and
a wide one costs three to four times that.

## The object-storage gap

**This is a local measurement and it does not answer the question R-015 asked.**

`mergeSchema` on raw Parquet opens every file's footer, so its cost on real
storage is dominated by per-file open latency, which this measurement contains
almost none of. On this machine the files were not merely on a local SSD, they
were **in the page cache** — written moments earlier and then read ten times per
condition. The numbers above are a CPU-and-deserialisation floor. On object
storage a footer open is a network round trip of tens of milliseconds, against
tens of microseconds here: a difference of roughly three orders of magnitude in
the per-open term.

So the local sweep establishes:

- the **shape** of the curve — linear in file count, sublinear in schema width,
  indifferent to divergence;
- the **floor** — what the merge costs when I/O is free;
- the **relative standing** of the four conditions, which is a property of how
  many footers each opens and therefore survives the change of storage.

It does **not** establish the absolute wall clock on S3, and no arithmetic here
can be presented as if it did.

### An extrapolation, labelled as one

The concurrency structure is readable from the Spark source on the classpath and
is not a guess. `SchemaMergeUtils.mergeSchemasInParallel` parallelises the file
list into `min(N, sparkContext.defaultParallelism)` tasks, and within each task
`ParquetFileFormat.readParquetFootersInParallel` uses
`ThreadUtils.parmap(partFiles, "readingParquetFooters", 8)`. On this machine
that is 11 x 8 = **88 concurrent footer opens**; on a cluster the outer factor is
total executor cores. Hadoop 3.4.1 defaults `fs.s3a.connection.maximum` to 500,
so the S3A connection pool would not cap 88.

**Assuming** 30 ms of first-byte latency per ranged GET and two GETs per footer
(a tail read for the footer length, then the footer body; Spark builds a
`FileStatus` on the executor side, which is *expected* to spare S3A the HEAD,
though that was not verified through Parquet's `HadoopInputFile`), the added
latency term is

    N x 60 ms / 88 = N x 0.68 ms

which gives, *as an extrapolation and not a measurement*:

| Files | Added latency term | Local floor (narrow / wide) | Extrapolated S3 total (narrow / wide) |
| ---: | ---: | ---: | ---: |
| 1024 | ~0.70 s | 0.15 s / 0.35 s | **~0.85 s / ~1.05 s** |
| 4096 | ~2.8 s | 0.33 s / 1.51 s | **~3.1 s / ~4.3 s** |

Per resource type, at `local[*]` parallelism. Every figure in the last column is
arithmetic over an assumed latency, not something anyone observed. A cluster
with more cores would divide it down; a higher-latency store or a smaller driver
would push it up.

File **listing** is not the differentiator. A paginated S3 LIST returns 1000 keys
per request, so 4096 files is five sequential round trips, of the same order as
the 151 ms the supplied-schema condition cost locally. Listing is paid by the
fallback too. Footer opens are what separate the conditions, on any storage.

### What would close the gap

Either of these, and neither was in scope here:

1. **A synthetic-latency filesystem** — a `FilterFileSystem` over
   `LocalFileSystem` registered under its own scheme, with an injected delay in
   `open()`. This would turn the extrapolation into a measurement and, more
   usefully, would *validate* the 88-way concurrency structure rather than
   assume it. It is the cheaper of the two and is the one to do if the question
   is reopened.
2. **A real object-store harness** — MinIO or testcontainers plus `hadoop-aws`,
   which measures the real thing including request retries, throttling and
   connection pool behaviour that no local model reproduces.

## The gate

**Does merge cost force a fallback for raw files? On the local evidence: no, but
the evidence does not reach far enough to clear the default at the wide end
either.** Stated precisely:

- **For Delta, the gate is answered and does not bite.** 2–6 ms, flat in file
  count, on the path Pathling's own sinks write. Nothing here argues for
  changing anything about Delta reads.
- **For raw Parquet at the narrow end** — the pruned schema, which is the normal
  case — the local data supports keeping the default on. 0.06–0.09 ms per file of
  floor, extrapolating to under a second per resource type at 1024 files, is not
  a cost worth a correctness regression.
- **For raw Parquet at the wide end, at 4096 files, the local data does not
  clear it.** 1.5 s of pure CPU floor per resource type, extrapolating to
  perhaps 4 s on S3, multiplied by every resource type in the warehouse at
  `ParquetSource` construction, is large enough to matter and is not something a
  page-cache-warm local median can settle.
- **The fallback is cheap, but it is not free of the merge.** The
  supplied-schema condition costs listing and nothing more — 3–10x cheaper than
  merging locally, and dramatically cheaper on object storage, where it
  eliminates N round trips. What it needs is a schema that covers the union of
  the files, and for a corpus nobody has read yet that schema is either the
  output of a merge or the writer's own. The fallback is therefore a way to
  avoid merging *repeatedly*, not a way to avoid merging *at all*.

### What this means for T118

Three things, and the third is the one that would have cost a rewrite if T119
had run after T118 instead of before it.

1. **Keep merging on by default.** The measured alternative is not "faster", it
   is *wrong*: 6 of 24 leaf columns, silently.
2. **The opt-out must take a schema, not a boolean — and the schema has to come
   from somewhere.** An opt-out spelled `mergeSchema=false` hands the user the
   silent null-fill measured above and is a trap. A *supplied* schema avoids it
   only while that schema is a **superset of the union of every file's
   columns**; supply one narrower and columns are dropped exactly as silently,
   merely deterministically. That superset is not free for the asking:
   `SchemaBuilder.pruned` takes an *observed* structure, and the observed
   structure of a divergent corpus **is** the merge, while
   `SchemaBuilder.dense` needs no data but is bounded by the nesting, extension
   and open-type configuration, so it silently misses anything a writer with
   wider bounds produced. This harness supplied the full derived schema, which
   was the union by construction — the best case. So the cheap path for T118 is
   **merge once and persist the result, or take the writer's schema, and supply
   that** — not "derive a schema instead of merging". The measurement says the
   supply mechanism is worth having; it does not say the merge can be skipped
   on first contact with an unknown corpus.
3. **Resolve lazily, per resource type.** The eager
   `FileSource.buildResourceMap` multiplies whatever the per-type cost turns out
   to be by the number of types in the warehouse, before a single query runs. On
   any storage that multiplication is the larger lever, and it is one T118 can
   pull without knowing the S3 constant.

## What this does not answer

The scope is schema resolution at read. It says nothing about write-side
merging, which decision 29 also covers and which fails outright rather than
slowly. It says nothing about query execution over a merged schema — the cost of
reading through a schema wider than any one file carries is a different
measurement, and `baseline.md` is the yardstick for that.

It is also not a measurement of encoder-written files. The corpora carry a
derived schema and one row per file, which makes footers realistic in width and
column count but not in row-group count; a file with many row groups has a
proportionally larger footer, so a production warehouse's per-file merge cost
will exceed the floor here for reasons that have nothing to do with storage
latency.
