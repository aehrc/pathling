## Context

The `sql-on-fhir` submodule ships an implementation-agnostic SQL-on-FHIR
benchmark: a declarative Synthea dataset recipe + ViewDefinition cases + per-size
expected row counts (`benchmark.schema.json`), a Bun/JS reference materializer and
reference runner (`sof-js`), and a report contract (`benchmark-report.schema.json`).
The runner contract claims to be satisfiable "in any language," but the reference
runner locates its data by `import`-ing the JS materializer's `layout.js`
(`recipeHash = sha256(stableStringify(recipe)).slice(0,8)`), an algorithm specified
nowhere normative. A second, non-JS implementation is the cheapest way to expose
that and the other assumptions the spec inherited from its sole implementation.

Pathling is a deliberate adversary: JVM-based (cannot import the JS tooling),
Spark-based (large fixed startup; lazy evaluation), strongly typed, and
distributed-by-design (columnar encode is a first-class, expensive phase). The
predicted spec weaknesses are written up in
`sql-on-fhir/docs/superpowers/specs/2026-06-30-pathling-second-implementation-findings.md`;
this runner is the instrument that confirms them with numbers.

The API surfaces below were verified against the actual library-api / `pathling`
sources, and the timing-harness design was validated against the real
`QueryableDataSource` / `DataSource` / `DatasetSource` classes.

## Goals / Non-Goals

**Goals:**

- A Java runner (in the dedicated `sof-benchmark` module) and a Python runner (over
  the `pathling` package) that each satisfy the `sof-benchmark-runner` capability:
  manifest-first data location, single-view execute+extract timing with warmup
  discarded, separate load-phase measurement, the row-count correctness guard, and
  a schema-valid `benchmark-report.json`.
- Demonstrate finding F1's fix by locating data via `manifest.json` matching,
  with zero knowledge of the JS recipe hash.
- Produce evidence for findings F2 (startup/codegen/encode overhead dominates the
  small size tiers), F4 (load is the hidden cost), and the Java-vs-Python
  binding-layer overhead.

**Non-Goals:**

- No changes to the existing JMH microbenchmark in `benchmark/`.
- No changes to the `sql-on-fhir` spec or tooling (improvements remain proposals
  in the findings doc).
- No data generation: materialization stays the submodule's `bun run data` tool.
- No publishing/registration of Pathling on the implementations page.
- No new benchmark cases; no F3 reference-in-filter probe (separate follow-up).

## Decisions

### D1 — Manifest-first data discovery (not hash recomputation)

`ManifestLocator` scans `<dataRoot>/*/<size>/manifest.json`, parses each, and
selects the directory whose manifest matches `dataset.name`, the requested `size`,
and `dataset.sizes[size].population`. The manifest (written by the materializer)
already carries all three, so the JS canonicalization is never reproduced.

*Alternative considered:* port `stableStringify` + SHA-256 + 8-char truncation to
Java/Python to compute the same path. Rejected — fragile across languages and
undocumented; reproducing it would defeat the point of the spike, which is to show
the contract should not demand it.

### D2 — Both Java and Python runners, table/csv sink

The engine (library-runtime JVM) is identical for both APIs; the Python API is a
py4j wrapper. Building both isolates the binding-layer overhead as a bonus finding.
With a **table/csv sink** (Spark writes the result from executors — the design's
own recommended measurement boundary, "never a lazy count"), the driver language
barely affects the timed write, keeping Java and Python comparable; under a
collect-to-memory sink the Python py4j row marshalling would confound it.

### D3 — Clean phase separation under Spark laziness (Java)

`pc.read().ndjson(dir)` and `view(...).execute()` are lazy, so a naive first
`write()` would secretly include parse + encode. The load phase therefore:

```java
QueryableDataSource ndjson = pc.read().ndjson(datasetDir);
Dataset<Row> encoded = ndjson.read(subject).cache();   // DataSource.read(String): the lazy encoded ds
long inputRows = encoded.count();                       // ACTION: forces parse+encode, populates cache
DatasetSource cached = pc.read().datasets().dataset(subject, encoded);
```

`ds.read(subject)` (declared on `fhirpath/.../io/source/DataSource.java`) returns
the FHIR-encoded dataset — the same pipeline `PathlingBenchmarkState` builds by
hand. `count()` doubles as the report's `inputRows`. The timed loop then runs the
view against `cached`, so it measures view evaluation + write over an
`InMemoryRelation`, not NDJSON parse/encode. `QueryableDataSource.cache()` alone is
insufficient (still lazy, returns the wider `DataSource` type without `view()`, and
caches every resource type), so the explicit `read(subject).cache().count()` +
rebuild-as-`DatasetSource` path is used.

### D4 — Time `write()`, count separately

```java
Dataset<Row> r = cached.view(subject).json(viewJson).execute();
long t0 = nanoTime();
r.write().format(sink).mode(Overwrite).save(outDir);   // execute+extract, fused
sampleMs = (nanoTime()-t0)/1e6;
long outputRows = r.count();                            // untimed correctness guard
```

`count()` is never the timed metric (Catalyst would prune projected columns and
under-measure); `write()` forces full materialization. `execute` and `extract` are
**not separable** in a lazy engine — the write fuses them — so they are reported as
one combined timed region with `phases: ["execute","extract"]`, and `load` is
recorded separately (D3). This inseparability is itself recorded as a design note
feeding the findings.

### D5 — Warmup loop in a single JVM

Whole-stage codegen is cached in a JVM-static Guava cache, so warmup iterations in
the same process correctly absorb codegen + Catalyst warmup; subsequent measured
iterations reuse generated classes. The runner is a plain `main` (no JMH fork), so
the cache persists across warmup→measurement. Writes go to one fixed temp dir with
`SaveMode.Overwrite`. `spark.sql.shuffle.partitions` is set low (≈ cores) so the
tiny SoF inputs don't spawn ~200 tiny part-files and add scheduling noise.

### D6 — A dedicated module rather than reusing the JMH `benchmark` module

The runner lives in its own `sof-benchmark` module, fully separate from the JMH
`benchmark` module. The two share no Java code — only the `library-runtime` and
Spark dependencies any consumer declares — so co-locating them only forced one
module to carry the other's dependencies (JMH, `test-data`) and a single shaded
`mainClass`. The dedicated module shades
`au.csiro.pathling.sof.benchmark.SofBenchmarkRunner` as its own `mainClass` and a
`ServicesResourceTransformer` merges Spark's CSV/parquet `META-INF/services`,
which the write path depends on.

### D7 — JSON handling: Jackson to extract, Gson inside Pathling

`com.fasterxml.jackson.core:jackson-databind` is added to `sof-benchmark/pom.xml`
without a version (managed by the imported `jackson-bom` in the root pom). The
runner parses the benchmark file with Jackson (`readTree`), iterates `cases[]`, and
re-serializes each `case.view` node to a string via `writeValueAsString`. That
string is passed to `FhirViewQuery.json(String)`, which deserializes with Gson
internally — the hand-off is a plain JSON string, so the Jackson/Gson split is a
non-issue (never hand Pathling a Jackson node).

### D8 — Python runner mirrors the Java design

`sof-benchmark/python/sof_runner.py` uses `click` (same flags, matching the
Pathling Python CLI's `click>=8.1.7`), stdlib `json`/
`pathlib` for manifest discovery, `pc.read.ndjson(dir)`,
`ds.view(json=view_json_string)` → DataFrame, and `df.write.format(sink)
.mode("overwrite").save()` timed with `df.count()` as the untimed guard. Phase
separation uses the analogous Python accessors (`ds.read(resource)`,
`df.cache()`/`df.count()`, `pc.read.datasets().dataset(...)`) — the exact Python
method names are confirmed in `lib/python/pathling/datasource.py` at implementation
time (low risk; a thin wrapper over the same JVM API). It emits the identical
report structure with `implementation.name = "pathling-python"`.

### Report shape (both runners, per `benchmark-report.schema.json`)

```jsonc
{ "implementation": {"name":"pathling-java","version":"9.9.0-SNAPSHOT"},
  "benchmarkVersion": "<submodule git sha>",
  "environment": {"os":…, "java":…, "spark":…, "cores":…},
  "measurement": {"phases":["execute","extract"], "sink":"csv", "warmup":1, "iterations":5},
  "results": {"<benchmark.title>": {"size":"s", "fhirVersion":"4.0.1",
    "cases":[{"title":"condition flat","status":"ok"|"count_mismatch",
              "inputRows":N,"outputRows":M,"samplesMs":[…],"stats":{min,mean,median,max},
              "phaseSamplesMs":{"load":[…],"executeExtract":[…]}}]}}}
```

## Risks / Trade-offs

- **[Cross-environment Synthea determinism]** `clinical-flat`'s blessed counts
  (Condition s:6219, Observation s:4794) were recorded on another machine; a
  different OS/JVM may yield different counts. → Treat a `count_mismatch` as a
  recorded data point for the spec's §13.6 open question, not a build failure.
- **[Tiny size tiers vs Spark]** At `s`/`m`, Spark startup + codegen + encode may
  dwarf the query, so the headline number characterizes overhead, not throughput.
  → This is expected and is precisely finding F2; the report's separate `load` and
  the overhead fraction are the evidence, not a defect to hide.
- **[Python phase-separation accessors unconfirmed]** `ds.read(resource)` /
  `pc.read.datasets()` Python names assumed by analogy. → Confirm in
  `lib/python/pathling/datasource.py` before coding; fall back to caching the
  DataFrame returned by the view's input if the dataset-source builder is not
  exposed.
- **[Ivy cache staleness]** Python runs may pick up a stale library-runtime after
  a Java rebuild. → Clear `~/.ivy2*` before Python runs (known project gotcha).

## Open Questions

- Where the Python runner's dependencies are declared (a `requirements.txt`/`uv`
  vs. relying on the locally-built `pathling` wheel) — resolve at implementation
  against how `lib/python` tests are run.
- Whether to also capture a `--size m` run in v1 for a 2-point scaling read, or
  defer to a follow-up once `s` is green end-to-end.
