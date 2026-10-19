# 31 — The engine never fails on an invalid stored date or decimal

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-016, FR-035, FR-063. Decision 133; principles 2, 6 and 7 in `evidence/ingress-strictness/README.md`.

**What to build:** A date, dateTime, instant or time whose stored text the engine cannot parse should evaluate to empty, never raise an error. The same applies to a stored decimal. The engine should parse with the same grammar as ingest. Today `LowBoundaryForDateTime.call` (`fhirpath/src/main/java/au/csiro/pathling/sql/misc/LowBoundaryForDateTime.java:84`), and its siblings for the high boundary and for `time`, throw `IllegalArgumentException("Invalid date/dateTime format: …")`. The current ingest validates no lexical form, so one bad stored `birthDate` aborts every query that compares on that element, for as long as the value stays in the table. The error names the value but not the resource or source. Only comparisons fail: selecting, exporting or comparing the same value as a string succeeds. So whether a query works depends on how it was written.

Invalid text can still reach storage once ingest validates dates and decimals:

- PoF data written by other tools;
- data that bypassed ingest;
- the `keep` policy, a follow-up.

The engine therefore needs a defined behaviour regardless. The loss is reported where it can be attributed, at ingest, and the engine stays robust. Empty matches FHIRPath's own conversion functions, such as `toDateTime()` returning empty for text it cannot convert.

Recommended fix:

- The boundary functions return null for text that does not parse.
- The date and decimal grammars move to a module that both `io` and `fhirpath` can use (`fhir-schema` or `utilities`), and the engine parses with them from there. Ticket 16 reuses them for ingest validation, so that ingest and engine cannot disagree on what is valid.

**Unverified, to check first:** the engine decodes stored decimal text to `DECIMAL(32,6)`. If that cast returns null for bad text, an invalid decimal already drops out of results silently. The behaviour would then be right but undocumented and untested. Pin it with a test either way.

**Blocked by:** None — can start immediately

**Status:** needs-triage

- [ ] A comparison over an element holding an invalid stored date, dateTime, instant or time returns the result for an empty value, and does not fail the query. This is pinned by tests on both a singular and a repeating element.
- [ ] The behaviour of an invalid stored decimal is established, made empty if it is not, and pinned by a test.
- [ ] The engine parses dates and decimals with a grammar in a module that `io` can use, ready for ticket 16.
- [ ] The documentation states that an invalid stored value evaluates to empty.
