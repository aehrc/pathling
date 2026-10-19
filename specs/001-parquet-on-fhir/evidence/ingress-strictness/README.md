# Ingress strictness and fidelity

Evidence for decision 133 and ticket 08 (OQ-1), gathered 2026-10-17 to 2026-10-19. The principles and the
scope below were agreed with the owner, and decision 133 in `../decisions.md` records them. The probes that the
reference sections rest on are in this directory. Each has its raw output beside it, as a `.txt` file of the
same name.

**Reproducing.** The Python probes run on the project's PySpark 4.0.2:

```bash
PY=lib/python/.venv/bin/python
PYSPARK_PYTHON=$PY PYSPARK_DRIVER_PYTHON=$PY $PY <probe>.py
```

`HapiProbe.java` runs on the released 9.9.0 runtime jar, which bundles HAPI 8.10.0, with PySpark's jars on the
classpath:

```bash
java -cp "<library-runtime-9.9.0.jar>:<pyspark>/jars/*" HapiProbe.java fixtures.tsv
java -cp "<library-runtime-9.9.0.jar>:<pyspark>/jars/*" HapiProbe.java fixtures-lexical.tsv
```

## Principles (agreed with the owner, 2026-10-19)

These govern everything below.

1. **No data loss on compliant input.** This holds at every setting. "Compliant" means conforming to the layout:
   every element is described by the definitions and has the right type and cardinality, and each primitive's
   text is valid for its datatype. The line is drawn between the **datatype** and the **element**:
   - **Datatype-level constraints are checked wherever the layout or the engine depends on them (owner,
     2026-10-19, a scope cut).** These are what the FHIR datatype itself defines, taken from the definitions:
     its regex, its range, and for temporal types the calendar. Each value can be checked on its own, without
     context. Checks fall into three groups:
     - **Text with annotations** (`date`, `dateTime`, `instant`, `time`, `decimal`): checked. They are parsed
       anyway to compute the range and numeric annotations (FR-002, FR-004, FR-005), so a value that fails is
       already detected and must be reported. The check is the regex **and** a successful parse: `1980-02-30`
       matches the regex but is not a date.
     - **Not stored as text** (the integer family, `boolean`, `base64Binary`): checked. A value outside the stored
       type cannot be stored. This includes the range of `positiveInt` and `unsignedInt`.
     - **Text with nothing derived** (`string`, `markdown`, `code`, `id`, `uri`, `url`, `canonical`, `oid`,
       `uuid`, `xhtml`): **passed through unvalidated in this change.** They are stored exactly as received, and
       nothing in storage or the engine depends on their form. A `code` with stray whitespace or an `id` of 70
       characters is kept as it is and produces no finding. HAPI did not check these either. Validating them
       against their regexes is a follow-up feature, and it only adds checks.
   - **Element-level constraints are not checked.** These are what a resource or a profile adds to an element:
     terminology bindings and value-set membership (`Patient.gender` being one of `male`, `female`, `other` or
     `unknown`), minimum cardinality, invariants, profiles and reference resolution. They are a validator's
     concern, not ours. A value that breaks only an element-level constraint is compliant, is stored as it is,
     and produces no finding.

   This departs from 9.9.0. HAPI rejected a code outside a required binding whenever its model had generated an
   enum for that binding (`gender: "xyz"` or `"Male"`), so loads that 9.9.0 rejected for that reason will
   succeed. The release notes have to say so. An optional binding check driven by the definitions could restore
   it later (P3).

   The exceptions are a closed list, kept in one place:
   - `contained` resources and `Bundle` (layout decisions, FR-006, FR-007);
   - `base64Binary` whitespace;
   - byte identity: key order, whitespace, escaping and number formatting;
   - content beyond the configured bounds in dense mode (FR-017).

   Any other loss from compliant input is a bug.
2. **No invisible data loss, ever, on any input.** Visible means three things. It is **attributable**: it names
   the resource, the path and the value, not a column. It is **machine-readable**: a pipeline can act on it
   without parsing logs. It is **delivered without being asked for**: at least a summary reaches the caller on the
   driver, and executor logs do not count.
3. **Loss is distinct from change.** A value whose JSON type differs from the definition, but whose text is valid
   for the target type, is a change of encoding and not a loss. For example, `"family": 123` is stored as `"123"`.
   This holds only if the token's text is kept exactly. A route that passes the number through a double, so that
   `1.50` becomes `"1.5"`, has lost data. A change is recorded at a lower severity than a loss (FR-063 forbids
   silent coercion).
4. **Fail early when validation is not satisfied.** Early means three things. It is **before commit**: no partial
   output survives a failure. It is **located**: the failure names the resource, the path and the value. It
   reports **all errors, not just the first**: a validate-only run collects every finding without writing
   anything.
5. **Tolerance is explicit and never becomes silence.** The user opts into accepting non-conformance. Doing so
   turns a failure into a recorded finding, never into nothing. Failing is therefore the default. That reverses
   decisions 44 and 68 ("ignore, never fail"), and the reversal has to be recorded as a new decision and
   announced in the release notes.
6. **The same input gives the same outcome.** This holds across formats (JSON, XML and bundles, FR-060), across
   whatever other rows share a file or partition, and across runs. Decision 68's decimal path breaks it, because
   whether `100` is stored as `100` or as `100.0` depends on the rest of the file. Column-level inference breaks
   it too.

7. **One grammar, validated at ingest, and the engine never fails on a stored value (owner, 2026-10-19).**
   Ingest and the engine parse dates, times and decimals with the same code, kept in a module both can use
   (`fhir-schema` or `utilities`). Ingest is where a bad value is detected and reported, because only there can
   it be attributed. A stored value the engine cannot parse evaluates to empty, never to an error. Such values
   can still arrive from PoF data written by other tools, or once the `keep` policy exists. Today's engine breaks
   this principle: see ticket 31.


## Two dimensions (2026-10-19)

**Repair** decides how far a value that does not match the expected encoding is converted to fit. Every
conversion that succeeds is lossless and is recorded as a change.

| Level | Accepts | Scope |
|---|---|---|
| `none` | Only the exact FHIR encoding of the input format. In XML every value is attribute text, so reading it as its type is the encoding itself, not a repair | **This change** |
| `widen` | Conversions that cannot fail for any value: number to a string-family type keeping the token text, boolean to string, integer to decimal, a single value to a one-item array | Follow-up |
| `narrow` | Conversions that depend on the value: `"5"` to an integer, `"true"` to a boolean, a one-item array to a single value | Follow-up |

No level changes a value itself. Trimming, case-folding, reformatting a date, flattening nested arrays and picking
the first of several values are never offered.

**Invalid-value policy** decides what happens to whatever repair leaves non-conformant: `fail` (the default,
principle 5), `drop` (removed, with a finding recording the loss), or `keep` (stored as received, with a finding,
or dropped as a loss where the layout cannot hold it).

**Scope of this change (owner, 2026-10-19):** repair `none`, and the policies `fail` (the default) and `drop`, both
applied to invalid values and to extra elements alike. `keep`, `widen` and `narrow` are follow-up features. Each
of them only adds to what is accepted or stored. Datatype validation in this change covers the dates, decimals and
non-text types only. The text-only types are passed through (principle 1), and validating them is a follow-up.

**`null`, `{}` and `[]` are invalid under the policy (owner, 2026-10-19).** FHIR JSON forbids them, so they are
non-conformant like anything else: `fail` fails the load, and `drop` treats the element as absent and records a
finding. Treating them as absent is a candidate for a later repair level. One case is not covered by this rule:
a `null` that pads a repeating primitive's array so that it aligns with its `_x` partner (`"given": ["Ann", null],
"_given": [null, {…}]`) is conformant FHIR and is accepted. An empty string `""` counts as an empty value too,
and is invalid under the policy for every type, the text-only types included. It is a length check, not a regex.
A whitespace-only string such as `"   "` is regex territory, so it is passed through under the scope cut.

**`contained` is an exception and never fails (owner, 2026-10-19).** It is valid FHIR that the layout does not
store by design (FR-006, principle 1). Whatever the policy, it is dropped and recorded as an informational
finding.

**A `dateTime` or `instant` with a time but no timezone is invalid, per the regex (owner, 2026-10-19).** It is
handled under the policy like any other invalid value. Under `drop` the whole value is lost, which is a known pain
point for EHR extracts, where this is common, and must be documented. A follow-up should handle it, through the
`keep` policy or a named repair, with its range annotation widened by ±14 hours.

## Delivering findings (owner, 2026-10-19)

Findings are data, not log lines. Each row carries a `_findings` column, an array of `(path, kind, value)` whose
path includes array indices, and the row's resource id makes each finding attributable. Option B's parser fills
the column directly. Option A generates SQL that builds it.

**In this change:**

- **The `_findings` column on the encoded dataset**, opt-in, for pipelines to act on. It is never written into
  the stored layout.
- **A summary on the driver**, computed with `Dataset.observe` during the same action as the write, so no second
  pass is needed and retried tasks are not counted twice. It holds counts by kind and path and the number of
  resources affected. It is logged and returned from the write. Whether it can also carry a bounded sample of
  findings is untested.

**`fail` and validation (owner, 2026-10-19).**

- **`fail` stops at the first invalid value a task meets.** The error names the resource, the path and the value,
  for example through `raise_error` or the parser's own exception. The job aborts before anything is committed.
  It reports one error, from whichever task failed first.
- **A validate-only operation reads everything and writes nothing.** It returns every finding, with the `observe`
  summary. This meets principle 4's "all errors, not just the first" without making every strict load pay for
  two passes.

**Follow-ups:**

- a `<table>_findings` table written beside the sink;
- a debug aid that logs each finding at DEBUG level, built on the public Dataset API (`mapPartitions`).

**Not used: a `trace()`-style side-effect expression.** `TraceExpression` collects through a static registry on
the driver, so on a cluster its entries are lost silently. It logs on executors, it duplicates entries under
retries (#2594), and it uses internal Catalyst API, which FR-049 forbids in encoding.

## Extra elements (2026-10-19)

Content the definitions do not describe takes several forms:

- fields from another FHIR version, such as R5's `triggeredBy` in R4 data;
- vendor or pipeline keys;
- `fhir_comments`;
- keys named like the layout's own fields.

**In this change**, extras are detected per value and handled under the invalid-value policy. A source key named
like a layout field is always an extra, and never read as layout content.

**Storing extras is a follow-up.** The owner leans towards typed columns, in two parts:

- **D1, user-supplied definitions.** The user extends the definition set to describe the extra elements, which
  then count as described content. Their types come from definitions, so FR-008 holds, and the schema is stable.
  This uses the mechanism that already serves custom resource types such as `ViewDefinition`. Proposed to come
  first.
- **D2, types inferred from the data.** An opt-in fallback, with these consequences documented:
  - schema drift between batches, append conflicts (FR-042) and widening to string;
  - collisions when a later FHIR version defines the same name with a different type;
  - number text going through a double unless the parser keeps it;
  - an eager inference pass, which rules out streaming.

  Inferred columns must be marked as not FHIR, by field metadata or a naming rule, so that FHIRPath and egress
  can tell them apart.

Rejected as the main direction: a JSON overlay column (option A in the discussion), and VARIANT. Either could be
revisited if D2's drift proves unmanageable.

## Reference: HAPI 8.10.0 as Pathling 9.9.0 used it

9.9.0 parsed every row with `fhirContext.newJsonParser()` (`library-api/.../ResourceParser.java` at `v9.9.0`),
never setting an error handler, so the context default applied: `new LenientErrorHandler()`, with
`errorOnInvalidValue` and `errorOnInvalidExtension` both `true`. The HAPI object was then encoded, which dropped
`contained` silently (`SchemaTraversal.scala:284`) and cut decimals to scale 6.

Raw output: `hapi-8.10.0.txt`.

"Callback" means the error handler was called, which is what produces a warning in lenient mode. `loc=null` in the
raw output means the warning carried no location: HAPI names the element but not the path or the resource id.

| Fixture | Input | HAPI lenient (= 9.9.0) | HAPI strict |
|---|---|---|---|
| C0 decimal text | `1.50`, 44 digits, `1.0e2` | kept exactly, exponent normalised to `100`; **9.9.0's encoder then cut to scale 6** | same |
| C0 `_birthDate` only | extension, no value | kept | kept |
| C0 `_given` with `null` padding | aligned arrays | kept | kept |
| C2 `null`, `{}`, `[]` | | dropped, **no callback** | same |
| C2 `fhir_comments` | | dropped, no callback | same |
| C2 `""` | empty string | dropped + warning | **fail** |
| C3 unknown root / nested / R5 field | | dropped + warning (no path) | **fail** |
| C4 `"5"` for integer, `"1.50"` for decimal | | coerced by text, **no callback** | same, coerced silently |
| C4 `123` for string, `"true"` for boolean | | coerced by text, no callback | same |
| C4 object for string | | element dropped + "unknown element" warning | fail |
| C5 object for array | `name: {..}`, `given: "Ann"` | wrapped as array + warning | **fail** |
| C5 array for scalar | `gender: ["male","female"]` | **first kept**, rest dropped + warning | fail |
| C5 nested array | `given: [["a","b"]]` | **flattened silently** | same, silently |
| C6 bad date | `1980-13-45` | **fail** | fail |
| C6 unknown code on a required binding | `gender: "xyz"` | **fail** | fail |
| C6 integer overflow | `99999999999` | **fail** | fail |
| C6 extension with value and nested extensions | | **fail** | fail |
| C6 two choice types | `valueString` + `valueInteger` | first kept + warning | fail |
| C6 bad `id`, URI with a space | | kept, not validated | kept |
| C7 `contained` | | HAPI keeps; **9.9.0 encoder drops silently** | same |
| C7 no / unknown `resourceType` | | fail | fail |
| C1 malformed JSON | | fail | fail |
| C1 duplicate key | `"gender":"male","gender":"female"` | **last kept, silently** | same, silently |
| C1 top-level array, or text after the object | | fail | fail |
| C5 array for a string primitive | `"family": ["A","B"]` | first kept + warning | fail |

### What this says

HAPI has three tiers, not two:

1. **Coerced by text, always, silently.** A primitive whose JSON type is wrong but whose text is valid for the
   target type is accepted in both modes. This is also exactly how XML behaves, where every value is text.
2. **Structural problems**: unknown elements, cardinality, empty strings, duplicate choice types. Lenient repairs or
   drops with a warning; strict fails.
3. **Invalid values**: bad dates, codes outside a required enum, integer overflow, malformed extensions. These **fail
   in both modes by default**. Only `LenientErrorHandler.disableAllErrors()` turns them into warnings.

So 9.9.0 users already had: per-row job failure on any invalid value, warnings with no row or path for unknown
elements, and some silent losses (first-of-array, nested array flattening, contained, decimal scale 6).

### Constrained string types (`fixtures-lexical.tsv`, raw output `hapi-8.10.0-lexical.txt`)

HAPI validates only what its own Java types need in order to parse a value, so its checking is uneven.

- It fails, in both modes:
  - a date without padding, `2020-1-5`;
  - a decimal string, `"1.2.3"`;
  - bad base64;
  - a code on a required binding that is not in the value set, or differs only in case: `"Male"` for `gender`.
- It accepts silently, in both modes, although the FHIR regex or rules forbid it:
  - a `dateTime` with a time but no timezone;
  - a `date` carrying a time;
  - an `instant` that is a date only;
  - an `instant` with no timezone;
  - a `time` of `25:61:00`;
  - a code with leading, trailing or doubled spaces;
  - a `positiveInt` of 0 and an `unsignedInt` of -5.
- It drops, silently with no callback, a whitespace-only string.

### Can Spark's reader attribute findings to a value? (`sql_findings_probe.py`, output `sql_findings_probe.txt`)

Spark 4.0.2, reading in-memory strings.

| Input | Inferred schema, then a SQL transform | Explicit schema, `PERMISSIVE` with `_corrupt_record` |
|---|---|---|
| `"active": null` | **Indistinguishable from absent** | Indistinguishable from absent |
| `"name": {}` | **The field disappears from the inferred schema** | — |
| `"given": []` | Detectable | — |
| `"valueInteger": "5"` | The column widens to string, and the JSON type is lost for every row | **Row-level**: the whole raw line goes to `_corrupt_record`, with no path |
| `"family": 1.50` | Widens to string as `"1.5"`, with no trace | **Accepted silently as `"1.5"`**: a loss with no finding |
| `"given": "A"` | The column widens to string holding raw JSON | Row-level corrupt record, with no path |
| integer overflow | `bigint`; the transform can check its range | Row-level corrupt record, with no path |
| unknown key `foo` | A column exists, so it is detectable per row | **Skipped silently** |

**Partial results** (`partial_probe.py`, output `partial_probe.txt`). With an explicit schema, `PERMISSIVE` mode
and Spark 4.0's default `spark.sql.json.enablePartialResults=true`, a mismatched value is the only thing nulled.
Every other field of the row is kept, whatever the key order, nested mismatches included. With partial results
off, a nested mismatch loses the whole enclosing array. So the reader route can drop per value. What it cannot do
is say which value it dropped: the bad value reads as null, which is indistinguishable from absent, and the only
signal is the raw line in `_corrupt_record`. A diagnostic re-parse of just the rows that carry a corrupt record
(with `parse_json`, or a small checker of our own) could recover the path and the value cheaply. Three gaps remain:

- `null` is indistinguishable from absent;
- a number given for a string is accepted lossily and silently;
- an unknown key is skipped silently.

None of the three produces a corrupt record.

### What puts a row into `_corrupt_record` (`corrupt_probe.py`, output `corrupt_probe.txt`)

Spark 4.0.2, with an explicit schema in `PERMISSIVE` mode.

**Flagged.** In each case only the bad value is nulled, and the raw line is copied into `_corrupt_record`:

- malformed JSON, and a bare scalar at the root;
- a number field given anything but its own number type: `"5"` or `5.0` for an integer, an integer overflow, or
  `"1.5"` for a double;
- a boolean field given anything but a boolean: `"true"`, `1`;
- an array field given anything but an array;
- a struct field given anything but an object, including an array of objects.

**Accepted silently:**

- **Anything given for a `string` field is stored as its JSON text.** A number, a boolean, an object and an array
  all become strings, so `["male","female"]` becomes the string `["male","female"]`.
- A nested array declared as an array of strings: each inner array becomes one string.
- Numbers inside an array of strings.
- **A duplicate key keeps the last value.**
- **A top-level array is split into several rows**, so one line becomes several resources.
- **Text after the object is ignored.**
- `"NaN"` given for a double.
- Unknown keys, at any depth.

Consequence for A+. A read with an explicit schema would flag no shape mismatch on a primitive declared as
`string`, so A+ does not read that way. It reads with the inferred schema of the pre-processed documents, and
compares that schema with the definitions path by path (ticket 08). A disagreeing path has widened to `string`,
holding each row's JSON text, and the pre-pass markers make every row unambiguous. Corrupt records then come only
from malformed JSON, and they are reported rather than re-parsed. Every candidate has to reject duplicate keys,
top-level arrays and text after the object.

### VARIANT keeps what the reader loses (`variant_probe.py`, output `variant_probe.txt`)

`parse_json` distinguishes:

- JSON `null` (`is_variant_null`) from an absent key (SQL null);
- the token type: `"5"` is `STRING`, `5` is `BIGINT`;
- `{}` (`OBJECT<>`) and `[]` (`ARRAY<VOID>`);
- the keys at any depth (`variant_explode`).

It loses the decimal's text: `1.50` reads as `DECIMAL(2,1)` `1.5`, a value of 30 digits becomes a double, and
`1.0e2` becomes `100.0`. A transform built on `parse_json` and SQL generated from the definitions ("A-variant")
could therefore implement the agreed scope, except for the exact text of decimals. Its performance and its XML
route are unmeasured.

**XML cannot be parsed into VARIANT on Spark 4.0.2** (`xml_variant_probe.py`, output `xml_variant_probe.txt`):

- `from_xml(x, 'variant')` is rejected: the schema must be a struct;
- a variant field inside a `from_xml` struct schema comes back null, silently;
- the XML reader ignores `singleVariantColumn`.

A-variant therefore covers JSON only. XML needs another route, either the `from_xml` struct route from
`xml-native.md` or B's parser.

## Current branch (from the spec and `StrictnessCheck`, not run)

`PrimitiveConverters` validates no lexical form. Every string-family type, the date family included, is stored as
the text it arrived as. Two cases lose the value invisibly, becoming null with no finding:

- an integer beyond the range of an int (`try_cast`);
- invalid base64 (`try_to_binary`).

It never fails on content (decision 68). Unknown content, contained, shape mismatches: ignored, one warning per
**column path** found in the inferred schema, no row ids. Type contradiction: per OQ-1, the whole element is lost for
every row read with it. Invalid values: not checked beyond what Spark's cast does (null).

## Superseded: the first draft of levels

The first draft proposed three presets, `strict`, `lenient` and `permissive`, modelled on HAPI's handlers. It was
written before the principles were agreed, and is replaced by the two dimensions above: repair, and the
invalid-value policy.
