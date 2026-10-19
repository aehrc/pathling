# 08 — Spike: the ingest and egress mechanism for JSON and XML

**Spec:** `specs/001-parquet-on-fhir/spec.md` — OQ-1, FR-045, FR-049, FR-050, FR-060, FR-061, SC-011. Absorbs T078e, T124a, T124b, T124j, T132, T134g, T134h and T135.

**What to build:** A measured decision on how FHIR JSON and XML get into and out of the PoF layout, in both directions. There are two candidates:

- **(A)** Keep Spark's reader and transform the schema it infers, as today.
- **(B)** Write our own parser and renderer, driven by the definitions.

**Hard constraints:** FR-049 (no Catalyst imports in encoding) and FR-050 (no expression encoder, hand-built serde tree or FHIR object per row).

**Ranking:** First, which of OQ-1's losses each candidate removes:

- the decimal's lexical form (T124b, T132, T134h);
- the granularity of a contradicting value, and numbers stored as text without a finding (T124j);
- empty structures and null-only arrays on export (T134h);
- per-value findings and values a converter cannot store (T124a, T134g);
- the `_x` array without `x` that HAPI drops (T078e).

Second, how much code we own.

**Bar:** Encode and decode are no slower than at the PoF switch, within about 10% drift (SC-011). T135's comparison is folded into this measurement.

The spike also records what each candidate means for XML and bundles. Today's HAPI routes behave differently from JSON (T078e), use a reflection workaround for custom resource types, and parse twice. These feed ticket 16.

**Blocked by:** None — can start immediately

**Status:** ready-for-human

- [ ] Both candidates are prototyped far enough to measure encode and decode on the benchmark that SC-011 names, and to show which OQ-1 losses each removes. Prototypes are throwaway.
- [ ] Evidence, numbers and reproductions are saved under the spec's `evidence/`.
- [ ] The owner chooses A or B. The choice is recorded as a decision.
- [ ] OQ-1 in the spec is updated either way. Each loss is either removed, with its FR text tightened, or kept as a stated exception.
- [ ] Ticket 16's placeholder is rewritten with concrete acceptance criteria.
- [ ] Any thread that outlives this programme, such as the upstream Spark issue T132 describes, is handed to ticket 26.

## Comments

### Proposed candidate set and measurement plan (2026-10-19, awaiting owner approval)

The validation and tolerance scope agreed with the owner is in `evidence/ingress-strictness/README.md`.
Decision 133 records it. In short:

- principles 1–7;
- repair `none`;
- the policies `fail` (the default) and `drop`;
- datatype checks on dates, times, decimals and the non-text types, with the text-only types passed through;
- `null`, `{}`, `[]` and `""` invalid;
- `contained` reported and never failing;
- extra elements detected per value;
- a per-row `_findings` column and an `observe` summary;
- `fail` stopping at the first error, plus a validate-only run.

The probes in those notes show what Spark's own routes can and cannot see.

The candidates replace the original A/B pair. A0 is the performance floor. The rest are ranked on how much of the
agreed scope they meet, then on how much code we own, with SC-011 as a pass/fail bar.

| | Ingest | Egress | XML |
|---|---|---|---|
| **A0, the floor** | HEAD at `514f8f818e` as it stands. Spark's reader with an inferred schema, then the generated transform, with no per-value validation. All OQ-1 losses are accepted | HEAD: `to_json`, decimals through a double | The `from_xml` route from `xml-native.md`, extended beyond its Patient subset, with no per-value validation (owner, 2026-10-19). HEAD's HAPI route is measured beside it, as a reference only |
| **A+, pre- and post-processing** | A Jackson pass that marks number tokens so a decimal's text survives. The pre-pass also marks string tokens whose text begins with `[` or `{`. A read with the **inferred** schema of the pre-processed documents, which serves as the fit pass. The inferred schema is compared with the definitions path by path (owner, 2026-10-19). A path that agrees holds only conforming tokens, because any deviating row would have widened it, so it needs only the datatype checks. A path that disagrees has widened to `string`, holding each row's value as JSON text. The markers make every row unambiguous, and the path is resolved per value with `from_json` using the definitions' type. Unknown keys arrive as columns and are found per row. Corrupt records come only from malformed JSON. They are reported, not re-parsed: `fail` fails with the raw line, truncated, and its source file. `drop` drops the row with a `malformed` finding. A diagnostic re-parse of only the rows flagged corrupt. Generated SQL that does the datatype checks and builds `_findings` | `to_json`, plus a step that restores number text | The `from_xml` route, plus the same generated checks |
| **A-variant** | `parse_json`, then SQL generated from the definitions over the variant. This keeps `null` versus absent, token types, `{}` and `[]`, and keys at every depth | No route that keeps decimal text: it reuses A0's `to_json` or B's renderer | **None.** Spark 4.0.2 cannot parse XML into a variant, so this candidate is JSON only and another route has to serve XML (FR-060) |
| **B, our own parser and renderer** | A streaming parser driven by the definitions, producing rows and `_findings` in one pass | Our own renderer, which writes the stored text | A StAX parser on the same core |

**Known gaps going in.** These come from the probes and are to be confirmed by the prototypes.

- **A0** meets none of the agreed scope beyond what HEAD does today.
- **A+** extends the current code. It adds the pre-pass, and replaces nulling a disagreeing column with resolving
  it per value. Its pre-pass needs no definitions. It:
  - marks numbers and strings that begin with `[` or `{`;
  - records `null`, `{}`, `[]` and `""` as findings;
  - rejects duplicate keys, a top-level array, and text after the object.

  To measure: the cost of the pre-pass, which runs before the inference scan, so its output is cached or computed
  twice; and whether a disagreement deep in an array or struct widens an ancestor to `string`. That would force a
  `from_json` of the whole subtree for every row of a messy batch.
- **A-variant** loses the decimal's text (`1.50` becomes `1.5`; more than 38 digits or an exponent becomes a
  double). It is JSON only, and its plan size, one generated `variant_get` per element, is unmeasured.
- **B** has no known gap in scope. It owns the most code.

**Every candidate** must also reject three things that Spark's reader accepts silently: a duplicate key (the
last value wins), a top-level array (split into several rows) and text after the object. Whether `parse_json`
does is unverified.

**What is timed, per candidate.**

- The presence and fit pass, which FR-011's fitted schema needs, timed **separately** from the encode pass. Every
  candidate pays it, so it must not flatter or penalise one route.
- Encode, and decode.
- JSON lines, XML lines, and XML bundles in both the collection and the transaction shapes.
- Extensions off and on.

Use the pathling-compare harness, at size `l`. Compare against the PoF-switch bar in `evidence/t136-benchmark.md`,
not against 9.9.0. Measure A0 fresh on `514f8f818e`, from a detached worktree, following the harness README's
version and Ivy procedure. The existing `10.0.0-SNAPSHOT-7ea4358` result predates stage 1. Nothing else may
benchmark or run `mvn install` on the machine while measurements run.

**Coverage.** Each candidate runs the fixtures in `evidence/ingress-strictness/fixtures*.tsv`,
extended as needed. That gives a pass/fail table against the agreed decisions and the OQ-1 losses.
