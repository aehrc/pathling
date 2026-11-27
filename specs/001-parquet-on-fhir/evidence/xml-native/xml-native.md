# Spark-native FHIR XML

Decisions 83 and 84 route FHIR XML through HAPI, inside the function Spark runs, and plan.md
records that as the one exception to FR-050. The reason given in `XmlConversion` and in
`encoder-scope.md` is that Spark's own XML reader infers each element's shape from the text, so a
repeating element that occurs once reads as a single value. T078e asks whether the HAPI routes can
be brought to JSON ingest's behaviour.

This evidence asks a narrower question first: **can Spark's own XML support — `spark.read.xml()`,
`from_xml` and `to_xml` — read FHIR XML into the layout and write it back out, with the transform
expressed as SQL and no FHIR parser in the per-row plan?**

**Result: yes, by one route, with the narrative as the only element that needs special handling.**

| Question                                                        | Verdict                                                                                                                                                                                     |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Can Spark parse FHIR XML at all?                                | **Yes.** A primitive's `value` is an attribute, read as `_value`; `url` and `id` likewise.                                                                                                  |
| Is the inferred schema usable as it stands?                     | **No.** Cardinality follows the data, and types are guessed: a string `"2106-3"` infers as a timestamp.                                                                                     |
| Is it usable for presence only?                                 | **Yes**, with `inferSchema=false` and a full scan. Types, cardinality and field order then come from the definitions, as `toLayout` does for JSON.                                          |
| Can a generated SQL transform reshape the result to the layout? | **Yes.** Prototyped for a Patient subset, including primitive metadata groups aligned by position.                                                                                          |
| Does `spark.read.xml(path)` select the right rows?              | **No.** `rowTag` matches a resource nested in a resource of another type, and silently misses a namespace-prefixed document.                                                                |
| Does `from_xml` over one document per string?                   | **Yes.** It parses from the root, so nesting cannot leak, and the root can be filtered and checked for a prefix in SQL.                                                                     |
| Is the narrative read correctly?                                | **No, by any reader option.** Text is decoded while child tags are kept, so `&lt;b&gt;` and `<b>` become indistinguishable. Recoverable from the raw document on the `from_xml` route only. |
| Can `to_xml` write FHIR XML?                                    | **Yes.** HAPI reads it back equal to the source. `df.write.xml()` cannot, because it always wraps the rows in a root element.                                                               |

So the route is: **one document per string, `from_xml`, and SQL generated from the definitions.**
That is the contract `FhirXmlReader.read(type, Dataset<String>)` already has, so the HAPI parse in
`XmlConversion` and `BundleTransformer.xml()` is what it would replace. "SQL only" means SQL
_generated_ from the definitions: Spark SQL has neither recursion nor schema introspection, so no
hand-written query covers the resource types.

## Conditions

|         |                                                                                                                                                                                                        |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Commit  | `559fbd9dae` (`issue/2367`)                                                                                                                                                                            |
| Machine | Apple M3 Pro, 11 cores, 36 GB, macOS 27.0.1                                                                                                                                                            |
| JVM     | Java HotSpot 21.0.3+7-LTS-152                                                                                                                                                                          |
| Spark   | PySpark 4.0.1 (Scala 2.13), `local[1]`. **The project builds on 4.0.2; nothing was run on it.**                                                                                                        |
| HAPI    | 8.6.0, from the released `library-runtime` 9.6.0 jar, used only to write fixtures and to check output                                                                                                  |
| Data    | Hand-written fixtures inline in the scripts, chosen to hit each hazard. No corpus. The write fixture is read with `spark.read.json`, not `FhirJsonReader`, so its shape only approximates the layout's |

The scripts are in `scripts/` beside this file, and `xml-native.txt` beside this file is the transcript
of one run of all of them. See [Reproduction](#reproduction).

## Row selection

### `rowTag` is a text scan, not a parse

The file source finds rows with `XmlTokenizer`, a scanner over the raw characters. It looks for `<`
followed by the `rowTag`, accepting it only where whitespace, `>` or `/` follows; skips comments and
CDATA; then reads forward to the matching close tag, counting nested occurrences of the same name.
Only that span reaches the StAX parser. The rules were read from the class's bytecode and confirmed
case by case with `rowTag=Patient` (`rowtag.py`, section 1):

| Input                                                         | Row                                                                                            |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `<Patient …>`, with attributes or a line break after the name | Yes                                                                                            |
| Two `<Patient>` roots back to back in one file                | Yes, two                                                                                       |
| `<Patients>`, `<PatientX>`, `<patient>`                       | No                                                                                             |
| `<Patient>` inside a comment, CDATA, or escaped text          | No                                                                                             |
| A Patient `contained` in a Patient                            | No; it stays inside the outer row                                                              |
| **A Patient `contained` in an Observation**                   | **Yes**                                                                                        |
| **A Patient in `Parameters.parameter.resource`**              | **Yes**                                                                                        |
| **A Patient in a Bundle entry**                               | **Yes**, without its `fullUrl`, so a `urn:uuid` reference to it cannot be resolved             |
| **`<f:Patient xmlns:f="http://hl7.org/fhir">`**               | **No, silently.** `ignoreNamespace=true` does not help, because the scan compares literal text |

The scanner has no notion of depth. FHIR names resource types in UpperCamel and elements in
lowerCamel, so no element is mistaken for a resource; the leak is confined to resources nested in
resources. A file holding one resource type, as Pathling writes, is safe to read with its own type
as the `rowTag` (`hapi.py`, section 3), but the same read over two such directories returned a
Patient contained in an Observation as a Patient.

### `from_xml` parses from the root

`from_xml(value, schema)` hands the whole string to the parser and starts at the root, whatever it is
called. A `contained` resource is a nested field, never a row. A regular expression over the same
string yields the root's name and prefix, so documents of another type are skipped and prefixed
documents refused rather than read as empty resources (`rowtag.py`, section 3). This is the same
selection `XmlConversion` makes after parsing with HAPI.

### Multi-resource files

FHIR defines no multi-resource XML file. A document has one root; a Bundle is the standard way to
carry several resources; and Bulk Data's export output is NDJSON. Roots written back to back are not
well-formed XML, and only the file source's scanner accepts them. The advantage the file source has
there is therefore confined to non-standard input. Bundles need the whole Bundle parsed, with
`rowTag=Bundle` or with `from_xml`, and their entries exploded; resolving a `urn:uuid` reference to
an entry is a map lookup in SQL (`transform.py`, part 3a). Because a definition-derived schema
identifies the `Reference` columns, the resolution can be type-aware, which is what decision 83 found
a Jackson-only bundle explosion lacked.

## Schema inference

What Spark infers from FHIR XML (`inference.py`):

| Case                                      | Inferred                                          | Consequence                                                    |
| ----------------------------------------- | ------------------------------------------------- | -------------------------------------------------------------- |
| `name` occurs once in every row           | `struct`                                          | Cardinality follows the data                                   |
| One row has two `name`s                   | `array`                                           |                                                                |
| `identifier.value` is `"2106-3"`          | **`timestamp`**                                   | The value is mistyped and mangled                              |
| `birthDate` is `"1970-01-01"`             | `date`; `string` once a `"1970"` is in the column |                                                                |
| `multipleBirthInteger`, `active`          | `bigint`, `boolean`                               | Correct by accident                                            |
| Any of the above with `inferSchema=false` | `string`                                          | No mistyping; cardinality still follows the data               |
| A primitive carrying only an extension    | A struct with `extension` and no `_value`         |                                                                |
| `contained`                               | `struct<Observation: …>`                          | A path the definitions do not describe                         |
| `div`                                     | A struct of the tags it happens to contain        | See [Narrative](#narrative)                                    |
| `samplingRatio=0.1`                       | A path present in one row of 201 is missing       | Data loss, because a later parse ignores what its schema lacks |
| Every case                                | Fields sorted alphabetically, plus `_xmlns`       | Not definition order                                           |

**Inference is usable for presence only.** With `inferSchema=false` and `samplingRatio` left at 1,
it lists every path the data populates and mistypes nothing. Everything else is the definitions'
business. This is the shape of decision 70 for JSON — infer, then fit — and the scan it costs is the
one `spark.read().json()` already makes. A query cannot be planned without a schema, so the scan can
only be avoided with the dense schema of M6.

## The transform

`transform.py` prototypes the whole path for a Patient subset, with a stand-in for the definitions.

**Part 1** infers presence with `inferSchema=false`, rewrites the inferred schema against the
definitions — array where the element repeats, `_value` beside an extension-only primitive, `div` as
a string, unknown paths dropped and listed for reporting — parses with `from_xml`, and reshapes with
generated SQL: values unwrapped from `_value`, `_url` and `_id` to `url` and `id`, a primitive's
`_id` and `extension` moved to its `_x` group with positions aligned for a repeating one, fields in
definition order, and every structure guarded so that an absent one stays absent rather than
becoming `{}`. The three resources come out as their FHIR JSON form, `contained` reported as
undescribed, and the generated SQL is 3,080 characters.

**Part 2** shows the corrected re-parse is unnecessary. With `inferSchema=false` the inferred read
already holds every value as its source text, and only the array-or-single shape can be wrong. The
generator sees the inferred schema when it emits the SQL, so it wraps a single value in an array
where the definitions say the element repeats. One inference scan and one query suffice. The
prototype reads with `spark.read.xml(dataset)` for brevity; in practice the query step would be
`from_xml(value, <inferred DDL>)` over the string column, so that the raw document stays in the row
for the narrative.

Part 2 also found that **Spark's casts are not FHIR's**. `CAST('yes' AS BOOLEAN)` is `true`. Each
primitive needs a conversion to the FHIR lexical rules — a `CASE` on `'true'` and `'false'`,
regular expressions for integers, decimals and dates — and a value that fails it becomes a null and
a finding rather than a failed job.

### Behaviour differences found along the way

| Behaviour                                    | Spark                                                                 | Note                                                                                                                                                   |
| -------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Surrounding whitespace in an attribute value | Trimmed by default                                                    | `ignoreSurroundingSpaces=false` keeps it                                                                                                               |
| A newline inside an attribute value          | Kept when escaped as `&#10;`, which is how HAPI and `to_xml` write it | A raw newline is normalised to a space, as XML's attribute-value normalisation requires; `transform.py`'s base64 fixture carries one and still decodes |
| An invalid value under `FAILFAST`            | Fails the job, as the HAPI route does                                 |                                                                                                                                                        |
| An invalid value under `PERMISSIVE`          | Nulls the value and continues                                         | With the conversion above, a finding, which the HAPI route cannot give (T078e)                                                                         |
| A singular element given twice               | Last value wins, unreported                                           | Non-conformant input; HAPI's behaviour was not compared                                                                                                |
| Content the schema does not name             | Ignored, as HAPI ignores it                                           | Reportable here, by comparing inferred paths with the definitions                                                                                      |
| A decimal                                    | The source text is available                                          | Passing it through a double reproduces decision 68's stored text; parity with the JSON route was not checked byte for byte                             |

## Narrative

Every primitive in FHIR XML is an attribute, so its value is plain text and decoding it is the
whole job. A string with markup characters written by HAPI as
`<given value="Piotr&lt;x> &amp; &quot;y&quot;"/>` reads back as `Piotr<x> & "y"` (`hapi.py`,
section 1). The narrative is the exception: `Narrative.div` is XHTML embedded as child elements, and
the JSON string is a serialisation of that subtree, which no attribute holds.

| Reader                                                    | `<div xmlns="…"><p>Jane &lt;Smith&gt; &amp; co</p><!-- c --></div>` becomes                                                                                                                                                                                                                    |
| --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| HAPI                                                      | An `XhtmlNode` tree of element, text, comment and CDATA nodes, serialised again on output with text re-escaped. Meaning is kept; the text is not byte-exact (`&#169;` becomes `©`, whitespace is inserted before a comment and a CDATA section, and the single-line writer collapses newlines) |
| Spark, `div` inferred                                     | A struct of the tags present; mixed text goes to `_VALUE`, an array when there are several pieces, and children are grouped by name. Order is lost and whitespace trimmed, so XHTML cannot be rebuilt                                                                                          |
| Spark, `div` as `STRING`, or the `xs_any` wildcard column | `<p>Jane <Smith> & co</p>`: child tags written out, text decoded, wrapper, its attributes and comments dropped. Markup and text are indistinguishable, and a bare `&` makes it ill-formed                                                                                                      |

Neither option tried — a `STRING` field and the `xs_any` wildcard column — keeps an element's raw
XML. For a narrative whose text holds no `<`, `>` or `&`,
`STRING` loses only the wrapper; for one that does — a dose, a laboratory threshold, a company name —
the stored value is wrong rather than missing.

### Recovering it from the raw document

On the `from_xml` route the raw document is in the same row. A regular expression for the first
XHTML `div` closed immediately before `</text>`, used only where the parse reports a `div` of the
resource's own, copies the narrative byte for byte (`narrative.py`, section 2):

| Case                                                                | Result                                         | Detectable                                                            |
| ------------------------------------------------------------------- | ---------------------------------------------- | --------------------------------------------------------------------- |
| Nested `div`s, whitespace before `</text>`, attributes on the `div` | Exact                                          |                                                                       |
| A comment between `</div>` and `</text>`                            | Missed                                         | Yes: the parse sees a `div` and the expression does not               |
| Only a contained resource has a narrative                           | The contained one matched                      | Yes: the parse sees no `div` of the resource's own, so it is not used |
| Prefixed XHTML, `<h:div xmlns:h="…">`                               | Missed, **and the parse sees no `div` either** | Only by a separate check for a prefixed `div`                         |
| `</div></text>` inside a CDATA section                              | **Truncated**                                  | No                                                                    |

Rewriting the narrative as an attribute before parsing — `<div value="…"/>`, its text escaped
including newlines, carriage returns and tabs — reads back identical to the source, and makes `div`
infer as `struct<_value: string>` like any primitive (`narrative.py`, section 3). It locates the
narrative with the same expression and inherits the same cases. What it adds is that neither the
inferred schema nor the transform needs a special case for `div`.

### Options

| Option                                                 | Route      | Fidelity                                                                              | Cost                                                     |
| ------------------------------------------------------ | ---------- | ------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| 1. Store Spark's string, wrapper restored              | Either     | Wrong where the text holds `<`, `>` or `&`                                            | None                                                     |
| 2. Drop `text` and report it                           | Either     | Lost, reported. `div` is required in `Narrative`, so `status` alone would not conform | None                                                     |
| 3. Extract from the raw document, guarded              | `from_xml` | Exact, but for the CDATA case                                                         | One expression                                           |
| 3a. As 3, rewritten as an attribute before parsing     | `from_xml` | As 3                                                                                  | A rewritten copy of each document with a narrative       |
| 3b. As 3a, with the narrative found by a StAX pass     | `from_xml` | L2 equal on every case tried, including the four that defeat the regular expression   | A streaming parse and re-serialisation per document      |
| 4. A StAX UDF locating `Resource.text.div` by position | `from_xml` | Exact                                                                                 | Code, and a second, early-terminating parse per document |
| 5. HAPI for the narrative only                         | Either     | As today                                                                              | The per-row HAPI parse this is meant to remove           |

## The StAX pass (option 3b)

The target is **L2**: the output document and the input parse, with HAPI, to resources that are
equal under `equalsDeep`, with `contained` out of scope. `scripts/NarrativeCheck.java` is a
prototype of both passes, using StAX from the JDK API (Woodstox was the provider that ran, from the
`library-runtime` jar):

- **Pre-pass.** Streams the document, tracking the open elements. An XHTML-namespace `div` whose
  parent is `text`, with no `contained` ancestor, is copied out as a self-contained string and
  replaced by `<div value="…"/>`, with the element's own prefix and namespace. The string
  declares every namespace it uses, so a prefix bound on the root survives. CDATA is kept as
  CDATA, which matters: HAPI's `equalsDeep` tells a CDATA node from escaped text, so turning it
  into escaped text made the CDATA case unequal. The attribute is escaped by hand, with newline,
  carriage return and tab as references, because XML normalises them to spaces inside an attribute.
  The substitution uses a placeholder that is checked not to occur in the document.
- **Post-pass.** Streams the output of `to_xml`, and where a FHIR-namespace `div` with a `value`
  attribute sits under `text`, parses the attribute and copies its events in place of the element.

The cases that defeated the regular expression above (a comment before `</text>`, a prefixed `div`,
and `</div></text>` inside CDATA) are all handled, because the parser decides what is markup. The
fourth, a narrative only in a contained resource, is not taken for the resource's own, because the
pre-pass skips a `div` with a `contained` ancestor.
Results (`outputs/narrative-stax-*.out`, Java 21; the outputs do not record the Spark version, and
the environment above says every run was on PySpark 4.0.1):

| Check                                                                                       | Result                                                    |
| ------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| 14 hand-written documents: rewrite, restore, HAPI `equalsDeep` against the source          | All L2 equal                                              |
| `from_xml` of the rewritten document, `div` as `STRUCT<_value: STRING>`, 10 Patients       | `_value` identical to the captured string in all 10       |
| `to_xml` of that struct, restored, HAPI `equalsDeep` against the source                    | All 10 L2 equal                                           |
| 161 R4 spec examples (`io/src/test/resources/data/fhir-spec/R4`) written to XML by HAPI    | All 161 L2 equal after rewrite and restore                |
| The same, narrative taken from the XML against the one in the JSON                         | 113 exact, 40 differ only in whitespace, none otherwise   |

The whitespace differences come from the fixtures, not the pass: HAPI's XML writer collapses a
narrative's whitespace runs (`\n      <p>` becomes ` <p>`), so the XML never held what the JSON
did. The documents are L2 equal regardless. One resource in the corpus has a contained narrative,
which the pass reports and leaves as it was; the document still round-trips.

**What this does not show.**

- The corpus is HAPI-written XML made from JSON examples. It has no comments, no prefixes, no
  declarations and no hand-written whitespace, so it is not independent evidence about real-world
  XML. The hand-written cases are the only coverage of those, and they are few.
- Only the narrative was tested through Spark. The rest of the resource went through HAPI in the
  corpus run, and through a three-field Patient schema in the Spark run; the generated SQL for the
  full transform was not exercised on these inputs.
- Both passes are full streaming re-serialisations of the document, not a slice of it. The corpus
  run took about 40 ms for the two passes over 161 documents, against about 45 ms for one HAPI parse
  of the same documents, in a cold JVM. That is a smoke figure, not a benchmark, and it says
  nothing about large documents, where a Bundle holds many narratives.
- The check treats `equalsDeep` as the definition of L2. It is stricter than XML infoset equality
  in at least one way (CDATA against escaped text), so a document can be infoset-equal and still
  fail it.
- Woodstox ran on the Java side from the `library-runtime` jar, and Spark ran without that jar. The
  clash described under Writing was not retried in a single JVM.

## Bundles

A bundle is only an envelope (FR-007: `Bundle` is not stored), so the contract is per entry
resource: each resource of the requested type that `BundleTransformer` returns today must come out
L2 equal, with `urn:` references to other entries rewritten as it rewrites them (decision 83).

`scripts/NarrativeCheck.java bundles` prototypes splitting in the same streaming pass:

- The pass starts at the root and refuses a root that is not a `Bundle`. For each
  `Bundle/entry/resource/<Type>` it records the entry's `fullUrl`, the type, and the resource's
  `id` and `meta/versionId`. For the requested type it also captures the resource as a standalone
  document, with its narrative rewritten and the namespace redeclared on its root. A nested
  `Bundle` and an entry without a resource contribute nothing.
- A map from each `urn:` full URL to `Type/id[/_history/versionId]` is built from all entries,
  and applied to every `Reference` of each captured resource. In the prototype HAPI does the
  application, standing in for the SQL `transform` the generated schema would give.
- Each captured resource then follows the single-resource route, so bundles need no transform of
  their own.

Compared with the current transformer's logic (copied from its source, not the class itself), on
hand-written bundles and on the R4 bundle examples written to XML by HAPI
(`outputs/narrative-stax-bundles.out`): the same JSON, resource by resource, for every case but one.

| Case                                                                                         | Result                  |
| -------------------------------------------------------------------------------------------- | ----------------------- |
| `urn:uuid` references in a reference, an extension, a `note.authorReference`                 | Same                    |
| A URN naming no entry, an entry without an id, a plain `Practitioner/9`, a string `urn:uuid` | Same, left as written   |
| A versioned target (`meta.versionId`)                                                        | Same, `/_history/2`     |
| `http` full URLs, `#contained` references, a contained narrative                              | Same                    |
| An entry with only a `request`, a nested `Bundle`                                            | Same, nothing returned  |
| A namespace-prefixed bundle with a prefixed `div`                                            | Same                    |
| 12 R4 bundle examples (6 with `urn` references)                                              | Same                    |
| **An entry whose `fullUrl` is `urn:uuid:X` and whose resource `id` is `X`**                  | **Different, see below** |

**A defect found in the current route.** HAPI, parsing a bundle, replaces a resource's id with its
`urn:uuid` full URL when the two share the uuid, and the JSON writer omits it. Through
`BundleTransformer` the resource then **loses its `id`**, and a reference to it stays `urn:uuid:X`
instead of becoming `Patient/X`. `scripts/BundleIdProbe.java` (`outputs/bundle-id-probe.out`) shows the rule is exactly that: a `urn:uuid`
that differs from the id, an `http` full URL, and no full URL all keep the id. The split pass
keeps the id and rewrites the reference. Bundles whose uuids are also the resource ids are, I
believe, common (generated transaction bundles do this), but I did not check any real data.
Whether the current behaviour is intended is for the owner to say; it contradicts the rule decision
83 states.

**What this does not show.**

- The comparison is against a copy of the transformer's logic, not the class, and its output is
  JSON written by HAPI. Nothing ran on Spark: no `from_xml` over a bundle, no generated SQL for the
  `Reference` columns, and no `urn → id` map inside a row.
- The prototype captures only the requested type's resources, but reads the whole bundle once per
  type, as the current route does. Memory for a very large bundle was not measured.
- A bundle carrying the `urn → id` map on every row costs O(entries²) for a large transaction
  bundle. It is a risk, not a measurement.
- The corpus bundles are HAPI-written, so the `http`/`urn` shapes in them are HAPI's. Most examples
  have no `urn` full URLs, which is why the hand-written bundles carry the reference cases.
- `Parameters.parameter.resource` has the same shape as an entry but is outside FR-007 and was not
  handled.

## Files Pathling writes

`FhirXmlWriter` returns one document per row, written by HAPI with pretty printing off. HAPI escapes
a newline in an attribute and collapses the narrative's whitespace, so saved as text each line is
one resource. Both `spark.read.xml()`, pointed at one type's directory, and `spark.read.text()` with
`from_xml` read them correctly, the only difference being the narrative (`hapi.py`, section 3).
Neither has a declaration or a namespace prefix to contend with.

## Writing

`to_xml` writes FHIR XML from a structure in the layout's shape (`write.py`, `write_check.py`):

- Fields prefixed with an underscore become attributes, so `named_struct('_value', x)` writes
  `<x value="…"/>`, and `_url`, `_id` and `_xmlns` likewise.
- The `rowTag` option names the root, and a constant `_xmlns` field supplies the namespace.
- An array writes repeated elements. A primitive's `_x` group is merged back with
  `arrays_zip(x, _x)`, so `id`, `value` and `extension` land on the same element.
- Element order is the struct's field order, which the generator takes from the definitions.
- Attribute values are escaped correctly, including `&#xa;` for a newline.
- `map('indent', '')` writes one line per resource, followed by a newline to trim.
- The narrative is written as a placeholder and replaced with the stored string, which is already
  serialised XHTML: `replace(to_xml(…), '<div>__DIV__</div>', text.div)`.

HAPI parses the output back to a resource equal to the source, over a Patient carrying an escaped
narrative, a resource extension, a repeating primitive with an id and an extension, an
extension-only metadata group, a boolean and a value holding a newline, `<`, `&` and `"`. Binary
content was not covered: the fixture's `photo.data` is a base64 string passed through, where the
layout stores `base64Binary` as binary (see [Not established](#not-established)).

`df.write.xml()` is not usable alone. It wraps the rows in a root element, `<ROWS>` by default and
renamed through `rootTag` (whether it can be omitted was not tested), and adds a declaration, so a file is not a FHIR document; and it offers
no point at which to substitute the narrative. `to_xml` followed by `write.text()` produces what
`FhirXmlWriter` produces now.

Two differences from today's writer would be the generator's to handle: an all-null structure and a
positional null, which HAPI drops on its way from JSON to XML.

**Classpath.** The `library-runtime` 9.6.0 jar carries its own Woodstox, and with it on the driver
classpath `to_xml` fails with `Unrecognized property
'org.apache.hadoop.shaded.com.ctc.wstx.outputValidateStructure'`. The write and the HAPI check
therefore run in separate JVMs. How the current `library-runtime` shades or excludes Woodstox needs
checking before `to_xml` is used beside it.

## What this means

- The premise behind plan.md's FR-050 exception for XML is that no Spark-native path preserves FHIR
  semantics. This evidence supports it for `spark.read.xml()` with inference. For `from_xml` with a
  definition-derived transform it found no counterexample, over a Patient subset with hand-written
  fixtures on Spark 4.0.1 and without measuring scale; the narrative is the one element that still
  needs the raw document, and on this route it has it. Whether that is enough to revisit the
  exception is the owner's decision.
- T078e's differences between the HAPI routes and JSON ingest would mostly go: undescribed content
  can be reported, and an invalid value can be reported and the read continue.
- The transform repeats what `ResourceTransformer.toLayout` does for JSON. Two shapes are open: a
  generator of its own for XML-shaped input, or a structural step from XML's shape to JSON's
  followed by `toLayout` unchanged. The second is possible because an element carrying a `value`
  attribute is always a primitive, but `toLayout` would have to accept string leaves and arrays
  inferred as single values. JSON marks repetition with an array explicitly and types its numbers,
  so it may handle neither today; that was not checked.

## Not established

- **Scale.** The generated SQL was 3,080 characters for a Patient subset. Its size and planning
  cost for the large resource types, with recursive extensions and open `value[x]`, were not
  measured, nor was any timing against the HAPI route.
- **Spark 4.0.2.** Everything ran on PySpark 4.0.1.
- **Decimal parity.** That the JSON route stores the same text as `CAST(CAST(x AS DOUBLE) AS
STRING)` was assumed from decision 68, not compared.
- **Binary on output.** `base64Binary` stored as binary needs `base64(...)` on the way out, with
  the newlines removed that Spark 4 inserts every 76 characters while
  `spark.sql.chunkBase64String.enabled` is on. The write path was not tested with either.
- **A real corpus.** All fixtures are hand-written.
- **Woodstox** in the current `library-runtime`.

## Reproduction

Each script creates its own fixtures and takes paths as arguments. All need PySpark 4.0; `hapi.py`
and `write_check.py` also need a `library-runtime` jar, any released one, for HAPI.

```bash
cd specs/001-parquet-on-fhir/evidence/xml-native/scripts
python -I rowtag.py
python -I inference.py
python -I narrative.py
python -I transform.py
python -I hapi.py <library-runtime.jar>
python -I write.py <out-dir>
python -I write_check.py <library-runtime.jar> <out-dir>
```

The StAX pass needs the `library-runtime` jar and an `slf4j-api` jar (the one in PySpark's `jars`
will do) for HAPI. Java 21 runs it as a source file:

```bash
CP=<library-runtime.jar>:<slf4j-api.jar>
java -cp $CP NarrativeCheck.java edges
java -cp $CP NarrativeCheck.java corpus <repo>/io/src/test/resources/data/fhir-spec/R4
python -I narrative_stax.py <library-runtime.jar> <slf4j-api.jar> <scratch-dir>
java -cp $CP NarrativeCheck.java bundles <repo>/io/src/test/resources/data/fhir-spec/R4/Bundle.ndjson \
  <repo>/.local/work/r4-examples/extracted
java -cp $CP BundleIdProbe.java
```

The last command is not rerun: it was added afterwards to name how `outputs/bundle-id-probe.out` was produced.

The `bundles` run reads `.local/work/r4-examples/extracted`, a local, untracked extract of the R4
examples, so it cannot be rerun from a fresh checkout as written.
