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
