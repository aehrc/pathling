# CPU per row of design A on repeating primitives

Measured for `design/annotations-in-the-engine.md`, section 4.4 and option B in section 5,
which holds B as the fallback "if A is markedly slower". The question is the CPU cost, per row,
of pairing `x` with `_x` into `struct(value, meta)` at traversal (design A), against keeping
parallel arrays and zipping only at a lambda boundary (option B), against today's engine.

**Result: A is not markedly slower than B on dates, and about 0.6 to 0.85 s per 2M rows slower
on a plain repeating string, where it zips for nothing. Today's eager range is the larger cost
by far: on unannotated data it makes an output-only repeating date 4.1 to 4.6 times the bare
read, and A removes all of it.**

- **Repeating date under a repeating parent (`name.period.start`).** A is 1.1x bare when only
  output, against 4.6x for today's engine on unannotated data (1.05 s against 4.21 s). The
  plan of A contains no `date_start` where only output is needed. Today's plan contains
  `date_range` there, on unannotated data, whether the date is only output, after
  `where(family.exists())` or in a comparison.
- **Comparison on a repeating date.** A and B are the same cost. Where the data carries the
  stored `__start`, A reads the field and runs no parse (1.34 s against 1.56 s for B, within the
  noise, and 1.95 s for today). Where it does not, both run one `date_start` per element
  (8.06M calls for 2M rows) and take 3.4 to 3.8 s, which is today's cost too (4.4 s).
- **Repeating string (`name.given`).** This is where A pays. Zipping `given` with `_given`
  costs 1.44x to 1.72x bare, against 0.99x to 1.15x for B, and the engine today does not zip a
  string at all (0.94x to 1.40x). The cost is the pairing and the `.value` read back out of a
  struct, about 0.6 to 0.85 s on 2M rows with four names and two givens each, or roughly 0.2
  microseconds of CPU per element.
- That zip is the whole difference between A and B on strings, and it is what B avoids by
  construction. Whether it matters is a judgement for the review: it is a fixed cost per
  element, not a cost that grows with the annotations, and it applies only to a primitive that
  the design pairs. It does not apply to a date's annotations, which are either read or
  computed on demand.

## Conditions

| | |
| --- | --- |
| Commit | `0c94065ec3` (branch `issue/2832_41-42_decimal-and-annotations`, the jars as built there) |
| Spark | 4.0.2, `local[4]`, `-Xmx6g`, adaptive execution off |
| Machine | 11 logical cores, runs serialised with `lockf` on the shared benchmark lock |
| Data | 2,000,000 Patients (1,000 synthetic patients multiplied by 2,000), 3 to 6 names each (mean about 4), 1 to 3 givens per name, a `period.start` on about 90% of names at five precisions, a `family` on about 90% |
| Layouts | `annotated`: `period._start` holds `__start` and `__end`, as `PathlingContext.encode` writes them. `plain`: range annotation off, so the engine computes `date_range`. Both carry a `_given` beside `given`, added by `WriteData.java`, because the encoder does not write one for a plain string. About one given in ten has an extension, the rest are null |
| Parquet | 85 MB annotated and 73 MB plain, dictionary-encoded and hot in the page cache, so the read is cheap and the CPU shows. Absolute times would be larger on data that does not repeat |
| Action | `write.format("noop")`, which evaluates the projection for every row |
| Statistic | Median of five timed runs after one warm-up run, in one JVM per arm and layout |

All arms are read in a shape where Spark reads the whole `name` column, as the engine does for
any path into a repeating element (see `annotation-pruning.md`), so the I/O is the same and the
differences are CPU. The bare read and the models are lambdas over `name`, not
`name.given` selected directly, which would prune to the field and read less.

## Arms

| Arm | How it ran |
| --- | --- |
| **today** | **Engine code.** A ViewDefinition column through `PathlingContext.read().parquet(...).view("Patient").json(...).execute()`, as `EnginePruningProbe.java` does. The paths are the FHIRPath expressions in the table below. |
| **bare** | Hand-built. Only reads the values, as the floor: `transform(name, n -> n.given)`, `transform(name, n -> n.period.start)`, a string compare for the comparison case. |
| **A** | Hand-built Spark columns. Each element becomes `struct(value, meta)` at traversal, paired by position as the engine pairs `x` and `_x` (`arrays_zip(given, coalesce(_given, array()))` for the string, `struct(period.start, resolve_or_null(period, "_start"))` for the date). Output projects `.value`. The comparison uses the real `ColumnFunctions.resolveOrElse(meta, "__start", date_start(value))`, with a Java UDF for `date_start` that parses with `FhirDateTime`, as `DateRangeKind` does. |
| **B** | Hand-built. The same without the pairing: output reads `given` and `period.start` directly, with nulls removed. The comparison zips `values` and `metas` at the lambda boundary, and then runs the same filter as A. The alignment of positions through null removal and flattening, which the design names as B's cost, is not modelled: B is given it for free. |

The three hand-built arms are not engine code. A and B reproduce the shape the design proposes,
not the code that would build it, so a real implementation can only add to their cost. The
`today` arm is the engine as it stands, which pairs and ranges dates and decimals (`traverseStored`)
and reads a string without any pairing.

## Results

Median wall time in seconds for the noop action over 2M rows, and relative to the bare read of
the same layout and case. Cases:

- `g1`: `name.given`
- `g2`: `name.where(family.exists()).given`
- `d1`: `name.period.start`
- `d2`: `name.where(family.exists()).period.start`
- `d3`: `name.period.start.where($this > @2000)`

Annotated data:

| Case | bare | B | A | today |
| --- | --- | --- | --- | --- |
| g1 output only | 1.39 (1.00x) | 1.38 (0.99x) | 2.00 (1.44x) | 1.31 (0.94x) |
| g2 after a lambda | 1.21 (1.00x) | 1.49 (1.23x) | 2.06 (1.70x) | 1.36 (1.12x) |
| d1 output only | 1.10 (1.00x) | 1.07 (0.97x) | 1.23 (1.12x) | 1.68 (1.53x) |
| d2 after a lambda | 1.01 (1.00x) | 1.35 (1.34x) | 1.34 (1.33x) | 1.65 (1.63x) |
| d3 comparison | 1.25 (1.00x) | 1.56 (1.25x) | 1.34 (1.07x) | 1.95 (1.56x) |

Unannotated data (the range is computed):

| Case | bare | B | A | today |
| --- | --- | --- | --- | --- |
| g1 output only | 1.06 (1.00x) | 1.22 (1.15x) | 1.82 (1.72x) | 1.48 (1.40x) |
| g2 after a lambda | 1.09 (1.00x) | 1.01 (0.93x) | 1.77 (1.62x) | 1.22 (1.12x) |
| d1 output only | 0.92 (1.00x) | 0.99 (1.08x) | 1.05 (1.14x) | **4.21 (4.58x)** |
| d2 after a lambda | 0.95 (1.00x) | 1.12 (1.18x) | 1.08 (1.14x) | **3.84 (4.04x)** |
| d3 comparison | 1.20 (1.00x) | 3.83 (3.19x) | 3.38 (2.82x) | 4.42 (3.68x) |

Spread: the minimum and maximum of the five runs are in `scripts/annotations-design/repeating-cpu/results-2M.txt`. A
run to run spread of 0.1 to 0.3 s on the bare and B arms is normal on this machine, so
differences of that size between them, such as `B` against `bare` in `g2`, are noise. The
difference between A and the others on `g1` and `g2` (0.6 to 0.85 s, with the spread of the five
runs of A under 0.1 s) is not.

### Where the range is computed

From the optimised plan (`df.queryExecution().optimizedPlan()`), and for the models from a call
counter in the UDF, which runs in-process in `local` mode.

| Arm, data | d1 output only | d2 after a lambda | d3 comparison |
| --- | --- | --- | --- |
| today, annotated | no `date_range` | no `date_range` | no `date_range` (reads `__start`) |
| today, unannotated | `date_range` in the plan | `date_range` in the plan | `date_range` in the plan |
| A, annotated | no `date_start` | no `date_start` | no `date_start`, 0 calls |
| A, unannotated | no `date_start` | no `date_start` | `date_start`, 8.06M calls (one per date) |
| B, as A | no | no | as A |

The `date_range` UDF is in the jar and was not wrapped, so its calls were not counted. The time
says one call per date: the unannotated `d1` costs 2.5 to 3.3 s more than the annotated or bare
read, and the 8.06M `date_start` calls of A cost 2.3 s over its `d1`. Today's `d1` with a
parse for every element, output only, is the finding 2 of ticket 43 measured: it is computed
under a repeating parent whether or not anything reads it, because the conversion sits inside the
lambda, where pruning does not reach. On annotated data the stored range is read, not computed,
and the cost is the engine's pairing and conversion to a System date, 1.5 to 1.6x bare.

## What this means for the design

1. **A is not markedly slower than B for dates, which are what the annotations are for.** A and
   B differ by less than the spread in `d1`, `d2` and `d3`. A pairs by position in the parent's
   lambda, where it needs the metadata anyway for the date. The projection of `.value` and the
   struct it comes out of cost 0.1 to 0.2 s per 2M rows (1.1x bare) on top of a read of the
   values. This does not support reconsidering B on cost.
2. **A pays on a plain repeating string**, 1.4x to 1.7x bare, against none for B and none today.
   The cost is the `arrays_zip` of two arrays per parent and the struct read back, for a value
   that nothing asked the metadata of. It is worth deciding whether strings are paired at
   traversal. If the
   review does not want that, the lighter form is to pair only where a primitive has a `_x` in
   the schema, which the engine already decides from the resolved type, so that a string with no
   stored group is read as it is today. That alternative was not measured.
3. **Today's eager range is the large cost, and the design removes it.** On unannotated data an
   output-only repeating date is 4.1 to 4.6x the bare read today, and 1.1x under A. That is the
   case for the accessors, not for the choice between A and B: B removes it too.
4. **On annotated data A's laziness gains nothing over today for output**, since the stored
   range is only read, never computed. Today is slower than A there (1.68 s against 1.23 s)
   because of its pairing and conversion to a System date, which A does not do until a
   comparison asks.
5. **Caveat.** The data is dictionary-encoded and the same 1,000 patients repeat, so Parquet's
   cost is low and the CPU shows; on real data a larger share of the time is I/O and the
   relative cost of the zip in `g1` and `g2` falls with it. The models do not include `removeNulls`
   across positions, `flatten` over the pairs of several parents, or the merge reconciler, so A
   can only cost more in the engine than here, and B's alignment is not paid for.

## Reproduction

All in `scripts/annotations-design/repeating-cpu/`, run from the worktree root, with
`THIRDPARTY_CP` set to the file listing Spark 4.0.2's classpath. `run-probe.sh` compiles the
probe against the branch's jars, because a probe launched from source cannot serialise its UDF to
Spark's tasks.

```bash
python3 gen_patients.py patients.ndjson 1000
./run-probe.sh WriteData.java patients.ndjson <data dir> 2000
./run-all.sh <data dir> results.txt <lock file>
```

`RepeatingCpuBench.java` takes `<data dir> <today|bare|A|B> <annotated|plain> [g1,d1,g2,d2,d3]`.
The raw `results-2M.txt` is beside the scripts (every line carries the median, minimum, maximum
and the plan and call counts). The data is under
`.local/work/issue/2832_41-42_decimal-and-annotations/spikes/repeating-cpu/`, which is ignored.
