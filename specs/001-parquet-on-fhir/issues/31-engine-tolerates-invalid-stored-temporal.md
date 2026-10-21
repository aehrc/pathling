# 31 — The engine never fails on an invalid stored date or decimal

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-016, FR-035, FR-063. Decision 133; principles 2, 6 and 7 in `evidence/ingress-strictness/README.md`.

**What to build:** A date, dateTime, instant or time whose stored text the engine cannot parse should evaluate to empty, never raise an error. The same applies to a stored decimal. The engine should parse with the same grammar as ingest. Today `LowBoundaryForDateTime.call` (`fhirpath/src/main/java/au/csiro/pathling/sql/misc/LowBoundaryForDateTime.java:84`), and its sibling for the high boundary throw `IllegalArgumentException("Invalid date/dateTime format: …")`. The two for `time` throw `IllegalArgumentException("Invalid time format: …")`. The current ingest validates no lexical form, so one bad stored `birthDate` aborts every query that compares on that element, for as long as the value stays in the table. The error names the value but not the resource or source. Only comparisons and date search fail: selecting, exporting or comparing the same value as a string succeeds. This is from reading the code, not a probe: the boundary functions are called only by `TemporalComparator` and by the search `DateMatcher`, which applies them to the stored element. So whether a query works depends on how it was written.

Invalid text reaches storage today, because the interim ingest validates no lexical form (decision 134). It will still reach storage once ticket 16 validates dates and decimals:

- PoF data written by other tools;
- data that bypassed ingest;
- the `keep` policy, a follow-up.

The engine therefore needs a defined behaviour regardless. The loss is reported where it can be attributed, at ingest, and the engine stays robust. Empty matches FHIRPath's own conversion functions, such as `toDateTime()` returning empty for text it cannot convert.

Recommended fix:

- The boundary functions return null for text that does not parse.
- The date and decimal grammars move to a module that both `io` and `fhirpath` can use (`fhir-schema` or `utilities`), and the engine parses with them from there. Ticket 16 reuses them for ingest validation, so that ingest and engine cannot disagree on what is valid.

**Decimals, measured by the ingest spike.** Probe 4 of `evidence/spike-08/README.md` (`probes/engine_decimal_probe.py`, on branch `issue/2831-spike-08`) found that the engine's `DECIMAL(32,6)` cast already reads unparseable text, such as `abc`, `1.2.3`, `""`, `NaN` and `1e400`, as empty, and nothing fails. That behaviour is right but undocumented and untested. The same probe found two cases that disagree with principle 7:

- text with surrounding whitespace, `" 1.5"` or `"1.5 "`, reads as `1.5`, so a value FHIR rejects reads as valid;
- a valid decimal of 40 digits reads as empty on the decimal path, yet `valueQuantity > 1 'mg'` is true for it, so the quantity path reads the value differently.

**The grammar (owner, 2026-10-21).** The shared grammar is lenient about the timezone: a `dateTime` or `instant` with a time but no timezone is valid, as `FhirPathDateTime` accepts it today. Ingest, stored values and date search arguments (`DateMatcher`) all use it. This reverses decision 133's rule that such a value is invalid, so nothing stored today becomes empty and no search that works today is rejected.

**Blocked by:** None — can start immediately

**Status:** needs-triage

- [ ] A comparison over an element holding an invalid stored date, dateTime, instant or time returns the result for an empty value, and does not fail the query. This is pinned by tests on both a singular and a repeating element.
- [ ] An invalid stored decimal evaluating to empty is pinned by a test. The whitespace and 40-digit cases are resolved, and pinned by tests.
- [ ] The engine parses dates and decimals with a grammar in a module that `io` can use, ready for ticket 16.
- [ ] The documentation states that an invalid stored value evaluates to empty.
