# 16 — DEFERRED: the ingest mechanism, per-value validation and lossless decimals

**Spec:** `specs/001-parquet-on-fhir/spec.md` — OQ-1, FR-002, FR-016, FR-019, FR-049, FR-050, FR-060, FR-063, SC-011. Decisions 133 and 134. Old task: T078e. T124c is likely made moot.

**What to build:** The ingest and egress mechanism ticket 08 measured, chosen and built, so that ingest meets decision 133's per-value scope and removes OQ-1's losses. Decision 134 deferred the choice until the owner has settled the requirements with colleagues: how much of decision 133's per-value scope is needed, and how much code the project is willing to own for it. Until then, ingest runs on the interim of decision 134, and ticket 32 takes XML and bundles off HAPI.

**The evidence to start from:** `evidence/spike-08/README.md`, "Summary of phase 2". It sets A+, A-variant and B, with B both through `Encoders.row` and through a Java UDF, side by side against decision 133's scope, OQ-1's losses, SC-011's bar and A0, and lists what was not measured. The prototypes, harness and batch scripts are under `evidence/spike-08/phase2/`. They were built on the stage-1 code at `514f8f818e`. Once ticket 32 lands, the XML baseline they compare against is no longer HEAD's HAPI route.

**What was not measured, should the choice need it:**

- B's XML encode under `hugeMethodLimit=8000` or through a UDF. B's XML rows go through `Encoders.row` too, so its XML figures may include the same uncompiled serializer that triples its JSON encode.
- B through a UDF on XML, bundles, the messy data and the fixtures.
- A UDF that returns zero or more rows to be exploded, which would need no non-deterministic flag and is the shape bundles need.
- JSON bundles, for any candidate.
- The formal JMH check of SC-011 (`evidence/t136-benchmark.md`).

**Blocked by:** The owner's choice of mechanism (decision 134). Ticket 32, because the rework replaces what it builds. Ticket 31, for the date grammar it puts in `fhir-schema`.

**Status:** needs-info

Criteria that hold whichever mechanism is chosen:

- [ ] The choice is recorded as a decision, and OQ-1's interim answer is replaced: each loss is removed, with its FR text tightened, or kept as a stated exception. FR-063's interim exception is removed or narrowed to match.
- [ ] No ingest or egress route parses into, or builds, a FHIR object per row, HAPI included (FR-061).
- [ ] XML and bundles keep, coerce, report and fail on exactly what JSON does (FR-060).
- [ ] Bundle references resolve to the target entry's relative reference, keeping its version (FR-062, decision 83).
- [ ] The rework carries over what was built on the interim ingest: the primitive metadata group of ticket 17, with its `_x` arrays that have no `x` beside them, and the dense mode of ticket 18. Both pass their own tests on the new mechanism.
- [ ] Encode and decode meet SC-011.
- [ ] Ingest validates dates and decimals with the shared grammar in `fhir-schema`, so ingest and engine agree on what is valid (decision 133, principle 7). Ticket 31 puts the date grammar there; the decimal grammar, and whether ingest validates per value at all, come with the follow-up issue that ticket 26 raises.
- [ ] Ingest's handling of non-conformant data, the modes of "Modes" in `evidence/ingress-strictness/README.md` among it, follows the follow-up issue that ticket 26 raises. #2367's ingest is `trust` (decision 133's addendum of 2026-10-22).
- [ ] If the stored form of any value changes from the interim's, such as a decimal's text, the layout column's version (ticket 29) is bumped, and data written by the interim is read or refused as that rule says.
- [ ] Any thread that outlives this programme, such as the upstream Spark issue T132 describes, is raised as a GitHub issue by this ticket. Ticket 26 closes before #2367 does, which does not wait for this one.

## Comments

### Rewritten as the deferred rework (decision 134, 2026-10-20)

Ticket 08 reported without a choice. This placeholder is now the deferred rework, and the work it held that does
not wait on the choice moved to ticket 32: XML and bundles without HAPI, the reflection workaround, and the XML
file format comment from 2026-10-17.

### XML already keeps a decimal's text (owner, 2026-10-22)

From ticket 32's review: the `from_xml` route stores a decimal as the text the XML carries and writes it back as
stored, so XML keeps a decimal's lexical form already. This rework need only bring JSON level with it.

### Line-delimited XML relies on Spark's XML writer to escape line breaks (2026-10-23)

From PR #2868's review: the line-delimited XML sink writes a document with no narrative and no empty element
exactly as Spark's `to_xml` wrote it. It stays on one line only because that writer, Hadoop's shaded copy of
Woodstox, writes a line break in an attribute value as `&#xa;` or `&#xd;`. A value such as `Annotation.text` or a
markdown description carries line breaks often. Any other StAX writer on the classpath fails `to_xml` outright,
because Spark sets two properties that only the shaded copy recognises
(`org.apache.hadoop.shaded.com.ctc.wstx.outputValidateStructure` and `outputValidateNames`), so the risk is a loud
failure, not silently split documents. `encoders/pom.xml` excludes Woodstox from the UCUM dependency for this
reason. `FhirXmlWriterTest.keepsALineBreakInAnAttributeToOneLineUnfinished` guards the escaping for now, and
would catch a Hadoop upgrade that changed it.

Whichever mechanism this rework chooses for egress should settle whether line-delimited correctness keeps resting
on Spark's writer and on keeping Woodstox off the classpath, which the server, Databricks and users' own
applications do not control, or whether Pathling escapes line breaks in attribute values itself, at the cost of a
pass over every document that the current fast path skips.

### Bundle splitting and XML preparation run twice per row (2026-10-23)

From PR #2868's review: the UDF that prepares an XML document that is not plain, or splits an XML bundle
(`FhirXmlReader`), and the one that splits a JSON bundle (`FhirJsonReader`), each run twice per input row. They
run once in the eager job that infers the schema, and again when the read's plan executes. On a large set of
bundles this roughly doubles the cost of splitting. Persisting the split dataset needs storage and a point at which
to unpersist it, which a library that returns a lazy `Dataset` does not have. Inferring the schema from a sample
drops fields that occur only outside the sample. The mechanism this rework chooses should parse each input once,
or state the cost of not doing so, and the benchmark should measure bundles so the cost shows.

### Spark's JSON reader refuses a string over 20M characters (2026-10-23)

From PR #2868's review: Spark's JSON reader, which `FhirJsonReader` uses, refuses any string longer than its
`maxStringLen` reader option, which defaults to Jackson's 20,000,000 characters. A resource with a large
attachment, such as a Binary or a DocumentReference whose base64 data is larger than about 15 MB, therefore fails
to read, from NDJSON and from a bundle alike, in Spark's schema inference. The bundle splitter itself no longer
reads the text of values that resolution does not need, so a large resource of a type not asked for no longer fails
the bundle. Whichever mechanism this rework chooses should settle the limit: lift it, at the risk of one huge
string exhausting an executor's memory, make it a reader setting, or keep it as a stated exception.
