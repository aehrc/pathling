# 31 — The engine handles invalid stored values in one of three modes: strict, tolerant and trust

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-016, FR-022, FR-023, FR-035, FR-063. Decisions 133 and 134; principles 5, 6 and 7 in `evidence/ingress-strictness/README.md`. Evidence: `evidence/engine-tolerance/README.md`, "Values". Ticket 33 is its counterpart for layout shapes.

**What to build:** A defined engine behaviour for a stored primitive value that is not valid for its type, chosen by a mode rather than by accident. The value is in a conformant schema, so the problem is only visible per value. This ticket covers values only. Layout shapes such as all-null structures and unaligned positional nulls are ticket 33's, and a schema that does not conform is out of scope.

Invalid values reach storage whatever ingest does:

- PoF data written by other tools, which Parquet constrains only to the physical type;
- data stored by the interim ingest, which validates no lexical form (decision 134);
- data that bypassed ingest;
- the `keep` policy, a follow-up.

**What the engine does today** (measured, see the evidence):

- An invalid date, dateTime, instant or time is present to the engine. `exists()` is true and select returns the text, but any comparison, `=` included, fails the whole query. The failure comes from the boundary UDFs that `TemporalComparator` calls for every comparison (`LowBoundaryForDateTime.java:84` and its siblings), and it names the value but not the resource or the path.
- An invalid decimal is absent. The decode to `DECIMAL(32,6)` returns null, so `exists()` is false and every operation is empty. This is undocumented and untested.
- There is no single grammar. The boundary UDFs check the calendar, `toDateTime()` and `toDate()` check only the pattern and return `2020-02-30` unchanged, the decimal decode trims whitespace, and the Quantity comparison decodes a decimal differently from the decimal itself.
- A date carrying a time, and a dateTime or instant with no timezone, are compared under an assumption rather than treated as invalid.

**The modes.** All three use one detector, FHIR's grammar for the type: the regex and, for temporal types, a successful calendar parse. It lives in `fhir-schema` (owner, 2026-10-21), a module that `io` and `fhirpath` can both use, so that ingest, `validate()` and the engine cannot disagree on what is valid. The modes differ only in what happens when the detector finds an invalid value.

| Mode | An invalid stored value | Cost |
|---|---|---|
| `strict` | The query fails, naming the resource id, the path and the stored text. Spark's `raise_error` can build that message from columns. | One branch on a value already decoded. |
| `tolerant` | Absent, the way an invalid decimal already is: `exists()` is false, select returns null, and every comparison, arithmetic, boundary and conversion over it is empty. | Free where the operation parses anyway. A check where it does not (see below). |
| `trust` | Not detected. The engine may use algorithms that do not parse the text, such as comparing `date` values as strings, and treats annotations as authoritative. Results on invalid input are unspecified and may be wrong. | None. |

On conformant data the three modes give identical results. Only non-conformant data tells them apart.

**Scope of the values.**

- `date`, `dateTime`, `instant` and `time` against FHIR's regex and the calendar. A date carrying a time is invalid. The grammar is lenient about the timezone (owner, 2026-10-21): a dateTime or instant with a time but no timezone is valid, as `FhirPathDateTime` accepts it today. Ingest, stored values and date search arguments (`DateMatcher`) all use it. This reverses decision 133's rule that such a value is invalid, so nothing stored today becomes absent and no search that works today is rejected. The named repair that widens the range annotation by ±14 hours for a missing timezone is ingest's follow-up, not the engine's.
- `decimal` against FHIR's decimal grammar. Probe 4 of `evidence/spike-08/README.md` (`probes/engine_decimal_probe.py`, on branch `issue/2831-spike-08`) found two cases that disagree with principle 7: text with surrounding whitespace, `" 1.5"` or `"1.5 "`, reads as `1.5`, so a value FHIR rejects reads as valid; and a valid decimal of 40 digits reads as empty on the decimal path, yet `valueQuantity > 1 'mg'` is true for it, so the Quantity path reads the value differently. How each is resolved is decided when the ticket is picked up (owner, 2026-10-21). Stop and ask the owner before changing the behaviour of either.
- An empty string in any primitive, invalid for every type as decision 133 has it.
- The range of `positiveInt` and `unsignedInt`, which their integer columns do not enforce.

**Rules that hold in every mode.**

- The mode is decided from configuration and the schema at planning time, never per row (FR-023).
- With a date or decimal annotation present (tickets 13 and 14), a null annotation beside non-null text marks the value invalid in Pathling's own output. For data from another writer it may equally be an annotation that writer did not compute, so `tolerant` decodes the text where the annotation is null, and `trust` takes the null as given.
- Conversion functions over arbitrary strings (`toDate()`, `toDateTime()`, `toDecimal()`, …) return empty for text that does not convert, in every mode, as FHIRPath defines them. They use the same grammar, so they stop accepting `2020-02-30`. The modes govern only the decoding of stored typed values.
- Export writes the stored text unchanged in every mode.
- Spark 4 runs with ANSI mode on by default, under which a plain `cast` of invalid text throws. The decode uses the `try_` forms, or Pathling's own parsing, so that `tolerant` does not depend on the session's settings.

**Out of scope.**

- A valid decimal beyond `DECIMAL(32,6)`, which the engine rounds or loses (FR-035). It is that requirement's consequence, and changing it is a separate question. That the decimal and Quantity paths disagree over such a value is in scope, and is decided at pickup, as above.
- A `strict` that refuses a source carrying no record of validation, which needs the validate-only run of ticket 16 and a marker defined for it.
- Whether annotations agree with their text, which only a validation pass can check.

**To settle before starting.**

- **The default mode.** Principle 5 points to `strict`, matching ingest's `fail`. It would refuse a query over any existing 9.9.0 table or foreign dataset that holds an invalid value, until that data is cleaned. `tolerant` keeps every query running. The answer goes in the decision below.
- **Whether `trust` earns its place.** Its only gain is the freedom to skip parsing. Measure it before building it (the benchmark criterion below). If the gain is negligible, drop `trust` and keep two modes.

**Trusting stored annotations (decision 135).** Whether the engine uses an annotation stored by another producer, or recomputes it, belongs to these modes rather than to the annotation step's settings. Ticket 11's *recompute* setting repairs stored data; it does not govern what the engine reads.

**Blocked by:** 13, 14. The annotation path needs the decimal and date annotations to exist.

**Status:** needs-triage

- [ ] A decision records the modes and the default, amending decision 133's principle 7, which says the engine never fails on a stored value. `strict` fails, so the principle becomes "never fails unless `strict` is chosen".
- [ ] One grammar for dates, times, instants and decimals lives in `fhir-schema`, ready for ticket 16 to use at ingest. The boundary functions, the decimal decode, the Quantity decode and the conversion functions all use it.
- [ ] In `tolerant`, an invalid stored value of each type in scope is absent: `exists()` false, select null, and comparisons, arithmetic and boundaries empty. Each type is pinned by tests on a singular and a repeating element, with and without annotations.
- [ ] In `strict`, the same values fail the query, naming the resource id, the path and the stored text.
- [ ] In `trust`, if kept, the full suite passes over conformant data, and the documentation states that results on non-conformant data are unspecified.
- [ ] The full test suite gives identical results in every mode over conformant data.
- [ ] The whitespace and 40-digit decimal cases are resolved as the owner decides at pickup, and pinned by tests.
- [ ] A dateTime and an instant with a time but no timezone are valid in every mode, and pinned by tests.
- [ ] `toDate()` and `toDateTime()` return empty for `2020-02-30`, `2020-13-45` and `…T25:00:00Z`.
- [ ] A benchmark over a few million values compares today's boundary UDFs with native decoding in each mode, for `date` and `dateTime`. It decides whether `trust` is kept, and its results are added to the evidence.
- [ ] The documentation states the modes, the default, and what each does with an invalid value.
