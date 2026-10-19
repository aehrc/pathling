# Runbook: spike 08 phase 2 at size `l`

This runs, unattended, every size-`l` measurement that phase 2 of spike 08 (ticket 08) needs. It is written for
a session that has not seen how the candidates were built. That is in `../README.md`, under "Phase 2".

**Machine.** Run it on a freshly restarted machine with every other application closed. The script refuses to
start while any Java process is running, and that includes IDE language servers such as `jdtls`. Nothing else
may benchmark, build or run Spark while it runs.

## What it runs

`batch-l.sh` runs in **slices, one per source**: JSON, XML lines, XML Bundles, transaction Bundles, and last
the messy JSON. A slice runs every route on its source, then appends its comparison to `slices.md`, so the
results arrive slice by slice rather than at the end. Each step is one `run-phases.sh` call for one route and
one source: the four types, and one warmup plus five iterations of each phase. The phases are fit, encode and
decode, as phase 1 defined them. Within a slice the routes are interleaved, so that a drift in the machine
does not fall on one route alone:

| Order in a slice | Label (results directory) | Route | Extensions |
|---|---|---|---|
| 1 | `p2-a0-r1-514f8f8` | A0 (`pathling`) | off and on |
| 2 | `p2-aplus-r1-514f8f8` | A+ | on |
| 3 | `p2-b-r1-514f8f8` | B | on |
| 4 | `p2-avariant-r1-514f8f8` | A-variant, JSON slice only | on |
| 5 to 8 | the same four as `-r2-` | | |

After the JSON slice comes **the noise gate** (below). The **messy slice** runs `p2-messy-{pathling,aplus,
avariant,b}-514f8f8` once each on `data/json-messy/l`, under the `drop` policy, extensions on. Each label holds
one results file per source and setting, so `p2-aplus-r1-514f8f8` ends with four, one per source.

**Extensions.** The extensions setting configures only the legacy encoders. A candidate fits the schema as the
data has it, so the candidates run with extensions on only, and no candidate reads the flag. A0 runs both
settings, as in phase 1. A0 does not read the flag on these paths either, so its two runs are replicates, and
an A0 label holds ten samples per source where a candidate label holds five. A candidate is therefore always
compared on its `r1` and `r2` pooled (ten samples) against one A0 label (ten samples).

The candidates run under the `fail` policy, which is decision 133's default and runs every check. The messy data
needs `drop`, because under `fail` its one bad document would stop the job.

**The noise gate.** After the JSON slice, `p2_spread.py` compares A0's `r1` and `r2` item by item: source, type
and phase, on the minimum of the ten samples each label has. It judges only items whose minimum is at least
1 s. The spread is acceptable when every judged ratio is within 0.85 to 1.15 and their median distance from 1
is at most 0.05. Otherwise the script stops with exit status 8 before any further slice. That outcome is
reported to the owner, not routed around. Setting `IGNORE_SPREAD=1` overrides it, but only on the owner's say.
The owner chose on 2026-10-19 to gate after the first slice, where the JSON candidates have already run, in
place of a 2-hour A0-only check before any candidate. The spread is reported after every later slice as well,
but only the JSON one stops the batch. `r2` brackets `r1` within every slice, so there is no closing A0 label.

**Progress.** `slices.md`, beside the logs, gains after each slice: each candidate against `p2-a0-r1` on that
source (pooled minimums and ratios, with and without A+'s and A-variant's py4j plan building), and A0's `r1`
against `r2`. Read it with `cat` from a terminal while the batch runs.

## Expected duration

About **6 hours**, and anywhere from 4.5 to 8.5. These are extrapolations, not measurements: nothing has been
timed at size `l` for phase 2. Each figure takes A0's phase 1 duration per source at size `l`, scales it by the
candidate's cost relative to A0 on Observation in the size-`s` smoke runs, and adds A+'s and A-variant's
plan-building time in Python, which does not grow with the data.

| Slice | Expected | Note |
|---|---|---|
| JSON, then the gate | about 2 h | A0 about 15 min per replicate; A-variant about 23 min per replicate is the largest share |
| XML lines, XML Bundles, transaction Bundles | 1 to 1.5 h each | A0, A+ and B, two replicates each |
| Messy JSON | about 35 min | 2, 8, 23 and 2 min for A0, A+, A-variant and B |

A+'s figures were estimated when its egress still overflowed the JVM's 64 KB method limit for Condition and
Encounter, which it no longer does at size `s` (README, deviation 6), so they are probably high. If the gate
stops the batch, it ends after about 2 hours. To tell slow from hung, look at the newest file in
`pathling-compare/results/<label>/`: each source and setting writes its `.json` when it finishes, and none of
them should take more than about 90 minutes.

## Prerequisites

Each of these is checked by the script, which stops with a message where one is missing.

- **The worktree** `/Users/szu004/dev/pathling/.claude/worktrees/issue/2831-spike-08`, on branch
  `issue/2831-spike-08`, at or after the commit that added this runbook.
- **The shared harness** `/Users/szu004/dev/pathling/.local/work/issue/2367/pathling-compare`. It is not in
  git. The script checks that each of its scripts is byte-identical to the copy committed in
  `phase2/harness/`, and stops (exit status 7) where one differs. To restore a script, copy it from
  `phase2/harness/` (`run-phases.sh` and the `*.py` at the root, the rest into `scripts/`).
- **The venv** `pathling-compare/venv-514f8f8`: Python 3.12.7, PySpark 4.0.4, and an editable install of
  `.claude/worktrees/bench-514f8f8/lib/python`. Every label uses it.
- **A0's jar.** `~/.m2/repository/au/csiro/pathling/library-runtime/10.0.0-SNAPSHOT/library-runtime-10.0.0-SNAPSHOT.jar`
  must have SHA-256 `3eb0a768fa6c27052d6eaa66f8499c30558aeab9b14d125277fd7571d54f2320` (commit `514f8f818e`).
  Where it does not (exit status 5), rebuild it in the clean worktree `.claude/worktrees/bench-514f8f8` with
  `mvn -q install -pl library-runtime -am -DskipTests`, and check the hash again.
- **The prototype jar** is built by the script, at every start, from the committed sources in `phase2/proto/`
  into `pathling-compare/tools/spike08-proto.jar`. It uses `javac` (JDK 21) against A0's jar and PySpark's
  jars, and records its hash. It installs nothing into `~/.m2`.
- **Data.**
  - The benchmark's JSON at `/Users/szu004/dev/sql-on-fhir.js/benchmark/data/synthea-clinical-wide/1/l`.
  - The harness's XML at `pathling-compare/data/{xml,xml-bundles,xml-bundles-txn}/l`.
  - The messy JSON at `pathling-compare/data/json-messy/l`. The script makes it with `make_messy.py` if it is
    missing, which takes about a minute.
- **Disk.** At least 15 GiB free before every step. The script stops (exit status 2) below that. About 48
  GiB was free on 2026-10-19.

## Command

```bash
cd /Users/szu004/dev/pathling/.claude/worktrees/issue/2831-spike-08
nohup specs/001-parquet-on-fhir/evidence/spike-08/phase2/batch-l.sh \
  > .local/work/issue/2831-spike-08/batch-p2.log 2>&1 &
```

To follow progress, run `cat .local/work/issue/2831-spike-08/batch-p2/slices.md` for the results so far, and
`grep === .local/work/issue/2831-spike-08/batch-p2.log` for the steps. Each step logs to
`.local/work/issue/2831-spike-08/batch-p2/<label>.<source>.log`, beside the machine's state before it
(`<label>.<source>.conditions-before.txt`). The results go to `pathling-compare/results/<label>/`.

Before each step the script checks the disk and waits up to 15 minutes for the one-minute load average to
fall under 3. It records swap, the load, the Java processes and the busiest processes.

## If it stops

| Exit status | Meaning | What to do |
|---|---|---|
| 2 | Under 15 GiB free | Free space, then resume |
| 3 | A step was started and is incomplete | Resume with `RESUME=1` |
| 4 | An A0 step failed (a failed candidate or messy step is recorded in `failed-labels.txt`, and the batch goes on) | Read its log. Fix nothing in the candidates without reporting it, because a fix changes what is measured |
| 5 | A0's jar in `~/.m2` is not `514f8f8`'s | Rebuild it as above |
| 6 | A Java process is running | Close it (`ALLOW_JAVA=1` goes on regardless, against the protocol) |
| 7 | A harness script differs from the committed copy | Restore it from `phase2/harness/` |
| 8 | The A0 replicate spread is too wide | Stop and report: `batch-p2/spread-json.txt`, `slices.md`, the conditions files, and the likely cause |

**Resume.** Run the same command with `RESUME=1` in front of `nohup`. A step whose results files
(`phases-<source>-ext<setting>.json`) all exist is complete and is skipped. An interrupted step continues from
its first unfinished setting: `run-phases.sh` skips the result files that already exist and redoes the one
that was interrupted. A resumed run appends to `slices.md` again for the slices it passes through. Never delete
a complete label. Results are never overwritten.

## Dry run

The first, unsliced sequence was dry-run on 2026-10-19 at size `s`, with Patient only and extensions off:

```bash
ALLOW_JAVA=1 SIZE=s PREFIX=dry IGNORE_SPREAD=1 QUIET_MINUTES=0 RESOURCES_OVERRIDE=Patient EXTS_OVERRIDE=0 \
  specs/001-parquet-on-fhir/evidence/spike-08/phase2/batch-l.sh
```

Every label ran and wrote its results:

- the messy labels read the messy data under `drop`;
- every label resolved A0's jar;
- a second run with `RESUME=1` skipped every label;
- a label left incomplete stopped the run with exit status 3 until `RESUME=1` finished it.

At size `s` nothing is over 1 s, so the spread check had nothing to judge. On the two phase 1 labels it judged
34 items and printed its verdict. The dry run's results are in `pathling-compare/results/dry-*`. They are not
measurements, and their prefix cannot collide with `p2-*`.

The sliced sequence was dry-run the same evening in the same way, with `PREFIX=dry2`:

- all 30 steps ran in slice order, each route interleaved as `r1` then `r2`, and each label ended with one
  results file per source;
- `slices.md` gained a section per slice, and the messy slice compared each candidate with A0's messy label;
- with one results file removed, a run without `RESUME=1` stopped with exit status 3 at that step, and a run
  with it reran that step alone and skipped the other 29.

With Patient only, `pooled_phases.py` fails on the types it expects and the report records its error, which is
how a failed step shows in `slices.md` too. At size `s` the gate had nothing to judge, and `IGNORE_SPREAD=1`
let the dry run go on. Its results are in `pathling-compare/results/dry2-*`.

## Afterwards

From `pathling-compare`:

```bash
python3 p2_spread.py p2-a0-r1-514f8f8 p2-a0-r2-514f8f8                     # noise, every source
for c in aplus b avariant; do
  for a0 in p2-a0-r1-514f8f8 p2-a0-r2-514f8f8; do                          # against each A0 replicate
    python3 pooled_phases.py $a0 "p2-$c-r?-514f8f8" --py4j
  done
  python3 pooled_phases.py pof-switch-26d7332 "p2-$c-r?-514f8f8" --py4j     # and against phase 1's bar label
done
for r in aplus avariant b; do python3 pooled_phases.py p2-messy-pathling-514f8f8 p2-messy-$r-514f8f8; done
python3 compare_phases.py p2-a0-r1-514f8f8 p2-{aplus,b,avariant}-r{1,2}-514f8f8
```

**Pool a candidate's two labels.** A candidate label holds five samples, where an A0 label holds ten, and the
minimum of fewer samples runs higher. So a candidate is compared only on its two labels pooled, which
`pooled_phases.py` does when given the quoted glob `p2-<candidate>-r?-514f8f8`, because it globs the label
into the results directory. A ratio from one candidate label alone is biased against the candidate. The messy
labels have five samples each, A0's included, so they compare with each other directly.

**Code generation.** Grep every label's logs and `.err` files for `Code grows beyond 64 KB`, and attribute
each one to the fit, encode or decode phase it occurred in. None occurred at size `s`, but the fitted schema at
size `l` is larger.

`--py4j` subtracts the time A+ and A-variant spend building their plans in Python, which is recorded per sample.
It is a prototype artefact: a JVM implementation builds the same plan in milliseconds. Report the figures both
with and without it.

Then copy each `results/p2-*` label (its `*.json`, `*.out`, `meta.json`, `conditions-*.txt`,
`resolved-jar.sha256` and `p2-notes-*.txt`) and the `batch-p2` logs into `evidence/spike-08/phase2/results/`.
Write the numbers into `../README.md` under "Phase 2", commit on `issue/2831-spike-08`, and report. Do not
update OQ-1, FR-063, ticket 16, the decisions or ticket 08's Status line.
