# 16 — PLACEHOLDER: ingest and egress rework per the spike

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-045, FR-060, FR-061, FR-062, FR-016, FR-019, FR-063, SC-001, SC-011. Old task: T078e. T124c is likely made moot.

**What to build:** Ingest and egress rebuilt on the mechanism ticket 08 chooses. This covers:

- XML in and out;
- bundles in both formats;
- JSON as well, if option B is chosen.

To be rewritten with concrete criteria once ticket 08 reports.

**Blocked by:** 08

**Status:** needs-triage

Criteria that hold either way:

- [ ] No ingest or egress route parses into, or builds, a FHIR object per row, HAPI included (FR-061).
- [ ] XML and bundles keep, coerce, report and fail on exactly what JSON does (FR-060).
- [ ] Bundle references resolve to the target entry's relative reference, keeping its version (FR-062, decision 83).
- [ ] An `_x` array with no `x` beside it is kept on every route.
- [ ] Encode and decode meet SC-011.
- [ ] The reflection workaround that finds custom resource types on an executor-side `FhirContext` is gone.
- [ ] Ingest validates dates and decimals with the grammar ticket 31 moved down, so ingest and engine agree on what is valid (decision 133, principle 7).

## Comments

### XML egress has no file format (found while baselining ticket 08, 2026-10-17)

XML egress produces strings and nothing can store them as files that read back. `decode(…, "application/fhir+xml")`
returns one document per row, and there is no XML sink: the sinks are NDJSON, Parquet, Delta and tables. Writing the
strings with Spark's `write.text` looks like it works but does not round-trip. The XML encoder writes a narrative's line
feed literally, which is valid XML, and every Synthea Patient narrative has one. On 9.9.0 at the benchmark's size `s`,
125 Patients became 250 lines, and encoding those lines back failed the whole job with `HAPI-1852: Failed to parse XML
content: Unexpected EOF; was expecting a close tag for element <div>`. Line feeds inside attribute values are already
escaped as `&#xa;`, so only element content is affected.

The current branch has the same gap, because its XML egress converts through the same encoder. The rework should
either:

- provide an XML sink, or a line-delimited XML form, that writes a remaining line feed as `&#xa;`, which parses back
  to the same character; or
- state that XML egress yields strings only, and document how to store them.

The benchmark harness writes its XML input the first way. See `.local/work/issue/2367/pathling-compare/README.md`,
"The XML data".
