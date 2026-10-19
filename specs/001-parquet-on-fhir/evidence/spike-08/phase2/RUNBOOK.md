# Runbook: spike 08 phase 2 at size `l`

This runs, unattended, every size-`l` measurement that phase 2 of spike 08 (ticket 08) needs. It is written for
a session that has not seen how the candidates were built. That is in `../README.md`, under "Phase 2".

**Machine.** Run it on a freshly restarted machine with every other application closed. The script refuses to
start while any Java process is running, and that includes IDE language servers such as `jdtls`. Nothing else
may benchmark, build or run Spark while it runs.

## What it runs

`batch-l.sh`, in this order. Each label is one `run-phases.sh` call: the four types, and one warmup plus five
iterations of each phase. The phases are fit, encode and decode, as phase 1 defined them.

**Extensions.** The extensions setting configures only the legacy encoders. A candidate fits the schema as the
data has it, so the candidates run with extensions on only, and no candidate reads the flag. Each candidate
therefore runs as two replicate labels, `r1` and `r2`, alternating across the candidates so that a drift in the
machine does not fall on one candidate alone. The messy labels run with extensions on only as well, A0's
included, so that they compare like with like. The A0 labels run both settings, as in phase 1. A0 does not read
the flag on these paths either, so its two runs are replicates, and each A0 label holds ten samples.

| Step | Label | Route | Sources |
|---|---|---|---|
| Noise check | `p2-a0-r1-514f8f8`, `p2-a0-r2-514f8f8` | A0 (`pathling`) | JSON, XML lines, both Bundle shapes |
| Gate | `p2_spread.py` on the two labels | | It stops here if the spread is too wide (below) |
| Candidates, replicate 1 | `p2-aplus-r1-514f8f8` | A+ | JSON, XML lines, both Bundle shapes |
| | `p2-b-r1-514f8f8` | B | JSON, XML lines, both Bundle shapes |
| | `p2-avariant-r1-514f8f8` | A-variant | JSON |
| Candidates, replicate 2 | `p2-{aplus,b,avariant}-r2-514f8f8` | as above | as above |
| Messy data (policy `drop`) | `p2-messy-{pathling,aplus,avariant,b}-514f8f8` | each | JSON (`data/json-messy/l`), extensions on |
| Closing A0 | `p2-a0-r3-514f8f8` | A0 | as above |

The candidates run under the `fail` policy, which is decision 133's default and runs every check. The messy data
needs `drop`, because under `fail` its one bad document would stop the job.

**The spread check.** `p2_spread.py` compares the two A0 noise-check labels item by item: source, type and
phase, on the minimum of the ten samples each label has. It judges only items whose minimum is at least 1 s.
The spread is acceptable when every judged ratio is within 0.85 to 1.15 and their median distance from 1 is
at most 0.05. Otherwise the script stops with exit status 8 before any candidate runs. That outcome is
reported to the owner, not routed around. Setting `IGNORE_SPREAD=1` overrides it, but only on the owner's say.

## Expected duration

About **7 hours**, and anywhere from 5.5 to 9.5. These are extrapolations, not measurements: nothing has been
timed at size `l` for phase 2. Each figure takes A0's phase 1 duration per source at size `l`, scales it by the
candidate's cost relative to A0 on Observation in the size-`s` smoke runs, and adds A+'s and A-variant's
plan-building time in Python, which does not grow with the data.

| Label | Expected | Plausible range | Note |
|---|---|---|---|
| `p2-a0-r1`, `p2-a0-r2`, `p2-a0-r3` | 60 min each | 55 to 70 | Phase 1 took 59 to 63 minutes |
| `p2-aplus-r1` and `-r2` | 1 h each | 0.75 to 2 h | Estimated when its egress still overflowed the JVM's 64 KB method limit for Condition and Encounter, which it no longer does at size `s` (README, deviation 6), so probably high |
| `p2-b-r1` and `-r2` | 12 min each | 8 to 23 | |
| `p2-avariant-r1` and `-r2` | 23 min each | 15 to 45 | Its encode at size `s` was about 20 times B's |
| `p2-messy-pathling`, `-aplus`, `-avariant`, `-b` | 2, 8, 23 and 2 min | | JSON only, extensions on only |

If the spread check stops the batch, it ends after about 2 hours. To tell slow from hung, look at the newest
file in `pathling-compare/results/<label>/`: each source and setting writes its `.json` when it finishes, and
none of them should take more than about 90 minutes.

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
- **Disk.** At least 15 GiB free before every label. The script stops (exit status 2) below that. About 48
  GiB was free on 2026-10-19.

## Command

```bash
cd /Users/szu004/dev/pathling/.claude/worktrees/issue/2831-spike-08
nohup specs/001-parquet-on-fhir/evidence/spike-08/phase2/batch-l.sh \
  > .local/work/issue/2831-spike-08/batch-p2.log 2>&1 &
```

To follow progress, run `grep === .local/work/issue/2831-spike-08/batch-p2.log`. Each label logs to
`.local/work/issue/2831-spike-08/batch-p2/<label>.log`, beside the machine's state before it
(`<label>.conditions-before.txt`). The results go to `pathling-compare/results/<label>/`.

Before each label the script checks the disk and waits up to 15 minutes for the one-minute load average to
fall under 3. It records swap, the load, the Java processes and the busiest processes.

## If it stops

| Exit status | Meaning | What to do |
|---|---|---|
| 2 | Under 15 GiB free | Free space, then resume |
| 3 | A label's directory exists but is incomplete | Resume with `RESUME=1` |
| 4 | A label failed | Read its log. Fix nothing in the candidates without reporting it, because a fix changes what is measured |
| 5 | A0's jar in `~/.m2` is not `514f8f8`'s | Rebuild it as above |
| 6 | A Java process is running | Close it (`ALLOW_JAVA=1` goes on regardless, against the protocol) |
| 7 | A harness script differs from the committed copy | Restore it from `phase2/harness/` |
| 8 | The A0 replicate spread is too wide | Stop and report: `batch-p2/spread.txt`, the conditions files, and the likely cause |

**Resume.** Run the same command with `RESUME=1` in front of `nohup`. A label whose `conditions-end.txt` exists
is complete and is skipped. An interrupted label continues from its first unfinished source and setting:
`run-phases.sh` skips the result files that already exist and redoes the step that was interrupted. Never delete
a complete label. Results are never overwritten.

## Dry run

The whole sequence was dry-run on 2026-10-19 at size `s`, with Patient only and extensions off:

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

## Afterwards

From `pathling-compare`:

```bash
python3 p2_spread.py p2-a0-r1-514f8f8 p2-a0-r2-514f8f8                     # noise check (also in batch-p2/spread.txt)
python3 p2_spread.py p2-a0-r1-514f8f8 p2-a0-r3-514f8f8                     # drift across the batch
for c in aplus b avariant; do
  python3 pooled_phases.py p2-a0-r3-514f8f8 "p2-$c-r?-514f8f8" --py4j       # each candidate against the fresh A0
  python3 pooled_phases.py pof-switch-26d7332 "p2-$c-r?-514f8f8" --py4j     # and against phase 1's bar label
done
for r in aplus avariant b; do python3 pooled_phases.py p2-messy-pathling-514f8f8 p2-messy-$r-514f8f8; done
python3 compare_phases.py p2-a0-r3-514f8f8 p2-{aplus,b,avariant}-r{1,2}-514f8f8
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
`resolved-jar.sha256` and `p2-notes.txt`) and the `batch-p2` logs into `evidence/spike-08/phase2/results/`.
Write the numbers into `../README.md` under "Phase 2", commit on `issue/2831-spike-08`, and report. Do not
update OQ-1, FR-063, ticket 16, the decisions or ticket 08's Status line.
