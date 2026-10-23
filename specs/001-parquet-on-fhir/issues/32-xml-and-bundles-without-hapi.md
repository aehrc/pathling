# 32 — XML and bundles without HAPI, on the interim ingest

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-045, FR-060, FR-061, FR-062, SC-011. Decision 134; decisions 83 and 84.

The spike's harness, fixtures and prototypes named below are on branch `issue/2831-spike-08` (`3e73682d01`). This branch carries only the pointer page `evidence/spike-08/README.md`. The pathling-compare harness is in the main checkout, under the gitignored `.local/work/issue/2367/pathling-compare/`. [ADR-0004](../../../docs/adr/0004-no-per-row-hapi-no-catalyst-in-encoding.md).

**What to build:** FHIR XML is read and written without HAPI, and so are bundles in both formats. This is the interim of decision 134: JSON stays on Spark's reader and the generated transform, which is A0, and the losses OQ-1 lists are accepted. The aim is to remove the dependency on HAPI, not to change what is stored.

- **XML in.** One document per string, parsed with `from_xml`. Presence and fit come from Spark's XML inference with `inferSchema=false`. Types, cardinality and field order come from the definitions, and a transform generated from them shapes the result to the layout. This replaces `XmlConversion`, which today parses each document with HAPI and writes it out as JSON.
- **XML out.** The stored layout is shaped back into an XML struct and written with `to_xml`. This replaces the HAPI writer in `FhirXmlWriter`.
- **Bundles in.** Each bundle is exploded to the resources of the requested type, and its `urn:` references to other entries are resolved as decision 83 states, with no HAPI parse per bundle. This applies to XML and JSON bundles alike, and replaces `BundleTransformer`.
- **JSON bundles (owner, 2026-10-21).** Each `entry.resource` is read as raw JSON text, kept if it is of the requested type, and run through the single-resource route. `urn:` references are then resolved on the typed `Reference` columns. This avoids one inference across resource types, which would widen a field that two types declare differently, such as `name`. Resolve through the bundle's entries rather than a `urn → id` map carried on every row, which xml-native.md flags as O(entries²). The route has no prototype: measure it against the HAPI route early.
- **An XML sink (owner, 2026-10-21).** A line-delimited XML sink writes one document per line, with a line feed remaining in element content written as `&#xa;`, and the XML source reads it back. See the comment below.
- **HAPI leaves ingest and egress.** `FhirParsers` and its reflection workaround, which finds custom resource types on an executor-side `FhirContext`, are removed. The FR-050 exception that `plan.md` records for XML and bundles is withdrawn.

**Interim behaviour, as decision 134 sets it.** The XML route stores what the JSON route stores, value for value: a decimal passes through a double, empty values are dropped as the JSON route drops them, and validation is by column type, reported once per column. It does not take on decision 133's per-value scope. Primitive ids and extensions are stored as the JSON route stores them until ticket 17.

This is a behaviour change for XML. HAPI fails the read on an invalid date, such as `1980-13-45`, and the `from_xml` route stores it as text, as interim JSON does. The engine runs in `trust` (ticket 31), so a query over such a value has undefined behaviour, and ticket 28 hands the release note on (owner, 2026-10-22).

**Classpath risk.** xml-native.md, "Classpath", found `to_xml` failing with the `library-runtime` jar on the classpath (`Unrecognized property …wstx.outputValidateStructure`). The jar bundles Woodstox without relocating it, and it clashes with Hadoop's shaded copy. Python and R load that jar, so the clash would show there. How it is fixed, by relocating or excluding Woodstox, is the implementer's choice.

**Starting points.**

- `evidence/xml-native/xml-native.md` establishes the route: row selection, inference, the transform, the narrative, bundles and writing. Its scripts are in `evidence/xml-native/scripts/`.
- `evidence/spike-08/harness/fromxml_route.py` is the Python prototype phase 1 measured, for all four benchmark types, lines and both bundle shapes, in and out. `gate_fromxml.py` is its correctness gate against HEAD. Both generate their expressions in Python over py4j, which is throwaway; the production transform is Java in `io`, built only from public Spark SQL API (FR-049).
- `evidence/spike-08/README.md`, phase 1, has its timings and the fixtures it was run against (`fixtures/fixtures-xml.tsv`). Against HEAD's HAPI route on the large types, it fitted lines at 0.50 to 0.61, encoded at 0.67 to 0.99 and decoded at 0.50 to 0.65.
- Phase 2's A+ XML route closed the namespace, trailing-text and second-root gaps below with a StAX pass per document (`evidence/spike-08/phase2/proto/src/au/csiro/pathling/io/Spike08XmlDocCheck.java`). It still took the narrative by regular expression.

**Gaps phase 1 found in the prototype, to close here.**

- A document with a prefixed FHIR namespace, which is valid FHIR, is left out silently, because rows are selected by the root's literal name.
- Text after the root element is ignored, and a second root is dropped, silently. A document with no namespace is accepted. Each of these is malformed and fails the read (FR-063).
- The narrative is taken from the raw document by regular expression, and xml-native.md lists the cases that defeat it: a contained narrative, a comment before `</text>`, a prefixed XHTML `div`, and CDATA. Use its StAX pass (option 3b). The target is L2, as xml-native.md defines it: the written document and the source parse to resources equal under HAPI's `equalsDeep` in the tests (owner, 2026-10-21). A narrative the pass cannot handle fails the read, naming the document. A narrative is never stored wrong silently.
- The bundle fit infers the whole bundle once per requested type, a constant of about 7.3 to 8.4 s per type at size `l`. That makes small types slower to fit than HAPI. One inference shared by all types would remove it, but would change decision 84's per-type `readBundles(type, bundles)`. The owner accepted the constant (2026-10-21): keep the per-type read, and record the small-type fit against SC-011.

**Out of scope:** the per-value checks, findings and policies of decision 133, and keeping the decimal's text. These are ticket 16's, and wait for the choice decision 134 defers.

**Blocked by:** None — can start immediately

**Status:** done

- [x] XML lines are read through `from_xml` and a transform generated from the definitions. For every resource type of the benchmark corpus and the R4 examples, the stored schema and rows equal the JSON route's for the same resources (FR-060). A decimal is compared by value, because XML keeps its text where JSON passes it through a double, and every other column exactly (owner, 2026-10-22).
- [x] XML is written through `to_xml`. Round-trip fidelity on the benchmark corpus is at least HEAD's.
- [x] XML and JSON bundles, in the collection and transaction shapes, are exploded without HAPI. References resolve as decision 83 states, keeping the target's version (FR-062). An entry whose `fullUrl` is `urn:uuid:X` and whose resource `id` is `X` keeps its id, and a reference to it becomes `Type/X`; the HAPI route loses both (xml-native.md, "A defect found in the current route").
- [x] A prefixed FHIR namespace is read. A missing namespace, text after the root and a second root fail the read, naming the document. A DTD fails too; anything else non-conformant at level 2 is ignored silently, and an element in another namespace is never read as FHIR (owner, 2026-10-22).
- [x] A narrative round-trips at L2, or the read fails naming the document. _A `div` that is not XHTML is now left out silently rather than failing (owner, 2026-10-22, item 7)._ There is a test for each of the four cases that defeat the regular expression in xml-native.md, and for its 14 hand-written StAX documents.
- [x] A custom resource type, such as `ViewDefinition`, is read and written in XML, single and in a bundle, through the definitions alone.
- [x] XML lines and bundles are read and written from Python and R on the shaded `library-runtime` jar, after the Ivy cache is cleared (`cache` and `jars` in every `~/.ivy2*` tree, then recreated).
- [x] No ingest or egress route imports HAPI's parser. `FhirParsers` and the reflection workaround are gone, and `CustomResourceTypeTest`'s guard on it is removed or replaced.
- [x] XML written through the line-delimited sink reads back through the XML source, including a narrative holding a line feed.
- [x] Encode and decode of XML lines and bundles are no slower than the bar on the pathling-compare harness at size `l` (SC-011), except the bundle fit's per-type constant, which the owner accepted. If any other phase is slower, the figures are reported to the owner before the ticket closes. _Patient XML lines at 1.09–1.11 were accepted as drift by the owner on 2026-10-22; ticket 10 measures XML lines against JSON in one session._
- [x] `plan.md`'s FR-050 exception for XML and bundles, and the HAPI routing of decisions 83 and 84, are amended to say they are withdrawn.

## Comments

### XML egress has no file format (moved from ticket 16, found while baselining ticket 08, 2026-10-17)

XML egress produces strings and nothing can store them as files that read back. `decode(…, "application/fhir+xml")`
returns one document per row, and there is no XML sink: the sinks are NDJSON, Parquet, Delta and tables. Writing the
strings with Spark's `write.text` looks like it works but does not round-trip. The XML encoder writes a narrative's line
feed literally, which is valid XML, and every Synthea Patient narrative has one. On 9.9.0 at the benchmark's size `s`,
125 Patients became 250 lines, and encoding those lines back failed the whole job with `HAPI-1852: Failed to parse XML
content: Unexpected EOF; was expecting a close tag for element <div>`. Line feeds inside attribute values are already
escaped as `&#xa;`, so only element content is affected.

`to_xml` may behave differently from HAPI here; check it. The route should either:

- provide an XML sink, or a line-delimited XML form, that writes a remaining line feed as `&#xa;`, which parses back
  to the same character; or
- state that XML egress yields strings only, and document how to store them.

The benchmark harness writes its XML input the first way. See `.local/work/issue/2367/pathling-compare/README.md`,
"The XML data".

### Implementation notes (agent, 2026-10-21)

Built on `issue/2831-xml-bundles`, from `issue/2831` at `cbce542dd5`. The route is the one this ticket sets, with
the changes below, which are for the owner to review.

**What was built.**

- *XML in* (`FhirXmlReader`, `XmlIngestShape`). One document per string. A streaming check of each document
  (`XmlDocuments`, `ResourceScan`) refuses what is malformed, naming it by type and id where it read them and by
  its first characters otherwise, and rewrites only what Spark would read wrongly: a prefixed FHIR namespace, and
  every narrative, which becomes the `value` attribute of its `div` (xml-native.md, option 3b). A document in the
  plain form most writers produce is recognised by a single character scan (`PlainFhirXml`), which rewrites its
  narratives in place, so the full StAX parse runs only outside that form. Spark's XML inference then learns
  presence, `from_xml` parses each document, and a plan generated from the definitions shapes the result into the
  JSON data model, which meets `toLayout` unchanged. Undescribed elements and attributes, and `contained`, are
  passed on under their names, so the transform reports them as it reports the same content in JSON (FR-006).
- *XML out* (`FhirXmlWriter`, `XmlEgressShape`). The stored layout is shaped into the XML structure and written
  with `to_xml`: positional nulls and structures that hold nothing are left out, a decimal is written from its
  double in plain form, and each narrative is written back as XHTML. All of that matches the previous writer, except
  that a narrative's whitespace is kept as stored rather than collapsed. A narrative that is not well-formed XHTML
  fails the write, naming the resource.
- *Bundles, both formats* (`XmlDocuments.split`, `JsonBundles`). One streaming pass per bundle takes out the
  resources of the type asked for: raw JSON text by offset for JSON, and a rewritten standalone document for XML.
  Each resource is paired with the resolutions of the URNs it mentions, `Type/id[/_history/v]`, from the bundle's
  entries. The resources are then read as documents of their own, and the resolutions are applied, through a hook in
  `toLayout`, to the stored `Reference.reference` columns only. No map of the whole bundle is carried on each row.
  The `fullUrl` `urn:uuid:X` / id `X` case keeps its id and resolves to `Type/X`.
- *The sink and source* are `FhirXmlWriter.write(type, stored, path, mode)`, one document per line, and
  `FhirXmlReader.read(type, path)`. A line feed in a narrative's text is written as `&#xa;` and a carriage return as
  `&#xd;`. A line break inside a narrative's comment, CDATA section or processing instruction cannot be escaped, so
  it fails the write, naming the resource.
- *HAPI is gone from `io`*: `FhirParsers`, `BundleTransformer` and `XmlConversion` are removed, and a Checkstyle
  import ban (`config/checkstyle/io-hapi-imports.xml`) keeps `ca.uhn.fhir.parser` and `ca.uhn.fhir.context` out of
  the main sources. It replaces `CustomResourceTypeTest`'s indirect guard on the reflection. `io` still declares
  `hapi-fhir-structures-r4` for the `FHIRDefinedType` enumeration. The JDK's StAX parser is used, built explicitly
  with DTDs and external entities off, so the routes do not depend on which StAX provider is on the classpath.
- *Woodstox.* The fix is an exclusion, not a relocation. `woodstox-core`, which reached `library-api` and the runtime
  jar through `encoders` → ucumate → jackson-dataformat-xml, registered itself as the StAX provider ahead of the
  copy Hadoop shades. `to_xml` sets a property only that copy recognises, so it failed in `library-api`'s tests as
  well as from Python and R. Relocating the classes would have kept the service registration, which is the
  problem, so `encoders` excludes `woodstox-core` from ucumate, and every module built on it leaves it out. The
  exclusion was first made in `library-api` alone, and moved to `encoders` after review so that it travels with the
  dependency that brings it. The runtime jar's `META-INF/services/javax.xml.stream.XMLOutputFactory` now names only
  Hadoop's copy. UCUM's XML reading works with any StAX implementation, and the `fhirpath` and `library-api` suites
  pass. An application that brings Woodstox in some other way must keep it behind Hadoop's copy on the classpath:
  forcing the unshaded provider fails every XML write with the same error.

**Evidence.**

- Criterion 1: `XmlJsonParityTest` reads every R4 example type (142, `Bundle` aside, `Parameters` included) and
  every type of the Synthea test bundle, from HAPI-written XML and from the JSON HAPI writes from that XML. It asserts
  equal stored schemas and rows. Narratives are compared as XHTML at L2, and every one of the 156 is also equal as
  text. At size `s` of the benchmark corpus (`clinical-wide`, all four types), the stored datasets of XML lines, XML
  collection and transaction bundles, and JSON collection and transaction bundles (made from the corpus, one per
  patient) are each equal to JSON lines' under `exceptAll` both ways, narratives byte for byte
  (`evidence/ticket-32/`, `gate.py` and `gate-s.txt`).
- Criterion 2: `FhirXmlWriterTest.writesWhatWasStored` checks every R4 type: the written XML parses to what the
  JSON writer writes (L2), and equals the previous writer's output but for narrative whitespace. On the harness at
  `l`, fidelity equals HEAD's on every source: 500/500 for Patient, Condition and Encounter, and 421/500 for
  Observation, where OQ-1's `0` → `0.0` decimal loss applies.
- Criteria 3–6, 8 and 9: `BundleReadTest` (both formats, both shapes, the decision 83 cases, the id defect, a
  prefixed bundle, and malformed bundles named), `FhirXmlReaderTest`, `XmlNarrativeTest` (the 13 single documents of
  `NarrativeCheck.edges()` plus its bundle, the four regex-defeating cases among them), `PlainFhirXmlTest`,
  `CustomResourceTypeTest` (`Widget`, single and in a bundle) and `PublicApiLayoutTest`
  (`encodesAndDecodesACustomResourceTypeAsXml`, a `ViewDefinition` single and in a bundle).
- Criterion 7: with `cache` and `jars` of `~/.ivy2.5.2` (the only `~/.ivy2*` tree) moved aside and recreated, and
  an empty `~/.ivy2/cache` and `~/.ivy2/jars` made, `mvn install -pl lib/python` passed 572 tests and
  `mvn install -pl lib/R` passed 211 expectations. Both include new tests that encode XML lines with a narrative
  holding a line feed, decode them back to XML, and resolve a transaction bundle's references. The jar Ivy resolved
  is the built one (SHA-256 `6dcd7b21…`). Python and R write XML through `PathlingContext.decode`, which is
  public Java API but has no Python or R wrapper: Python reaches it through `_jpc`, and R through `invoke`.
- Module suites at the final code: `fhir-schema` 41 run, 0 failed, 0 skipped; `io` 849, 0, 1; `fhirpath` 7393, 0,
  922; `library-api` 326, 0, 0. `encoders`, `terminology` and `utilities` are unchanged and were not run.

**SC-011** (`pathling-compare`, `run-phases.sh … pathling l`, label `phases-2831xml-8b1ee26`, against the bar
`phases-pof-switch-26d7332`). The figures are ratios of pooled minimums, with extensions off and on pooled. The
bar was measured in an earlier session, so the unchanged JSON rows, at 0.89–1.04, are this session's control.

| Source | Fit | Encode | Decode |
|---|---|---|---|
| XML lines, Condition, Encounter, Observation | 0.85–0.91 | 0.63–0.73 | 0.57–0.59 |
| XML lines, Patient | 1.09 (292 → 317 ms) | 1.10 (322 → 353 ms) | 1.11 (317 → 352 ms) |
| XML collection bundles | 0.47–0.90 | 0.56–0.78 | 0.65–1.03 |
| XML transaction bundles | 0.46–0.78 | 0.50–0.70 | 0.64–0.88 |
| JSON bundles (not on the harness; one session, extensions off) | 0.15–0.20 | 0.19–0.25 | 0.75–1.05 |

The accepted per-type bundle-fit constant **no longer exists**. Each bundle is split by a streaming pass, and only
the requested type's resources are inferred, so the Patient bundle fit is 0.46–0.47 of the bar rather than above it.
The one item above 1.0 beyond the JSON control's own spread is XML lines Patient, at 1.09–1.11, which is +25 to
+35 ms on a 0.3 s phase, single partition. It is reported here and not resolved. The SC-011 criterion is left
unticked for that reason: whether 1.09–1.11 against a bar from an earlier session is within the drift is the
owner's call.

The JSON-bundle figures come from scripts in `evidence/ticket-32/` (`make_json_bundles.py`,
`json_bundle_phases.py`), not the harness: the harness has no JSON bundles. The table above is
`evidence/ticket-32/outputs/cmp-8b1ee26.txt`. Bundles were made one per patient from the `l` corpus, and both builds
were run in the same session.

**For the owner to review.**

1. *Number columns are typed by marks, not by `inferSchema=true`.* JSON types a column of decimals as integers
   where every value is one: the R4 examples store `"5"` for dozens of `Quantity.value` columns, where a value
   always passed through a double would store `"5.0"`. That would break criterion 1. Spark's own type inference
   reproduces JSON's rule, but it cost 12.9 s against 2.7 s for the Observation scan at `l`, because it tries every
   string as a date and a timestamp (`evidence/ticket-32/outputs/inference-cost.txt`). So the scan keeps `inferSchema=false`, as the ticket says. Before it, a regular
   expression adds a mark attribute beside each value with a fraction or exponent (`pathling-fraction`) or more
   than 19 digits (`pathling-beyond-long`). The mark's presence in the inferred schema types the column as JSON
   would: double, decimal(38,0), or long. Only the scan sees marked documents.
2. *A non-number in a numeric column, or a non-boolean in a boolean one, is null per value and reported nowhere.*
   JSON nulls the whole column and logs one finding. The XML has no JSON form to compare with here, since JSON
   would have carried such a value as a string token, but this is a difference in what is stored for invalid input.
   An integer beyond a long is decimal(38,0) here and decimal(n,0) in JSON, which stores the same text.
3. *The sink and source are on `io` only.* There is no `DataSinkBuilder`/`DataSourceBuilder` entry, and Python and R
   write XML only by calling `PathlingContext.decode` through the gateway (`_jpc` in Python, `invoke` in R), as
   before, since neither wraps it. Adding public sinks was judged beyond what the ticket implies.
4. *Removing Woodstox from `encoders`* also removes it from what the server will inherit from the runtime, where
   HAPI's XML responses may rely on it.
5. *A `urn:` reference inside a primitive's extension* (`_status.extension.valueReference`) is not resolved, because
   primitive metadata is not stored until ticket 17. The previous encoder resolved it.
6. *A narrative is stored as this route's serialisation of the XHTML*, not as the source's bytes. XHTML is written in
   the default namespace, so `<h:div>` becomes `<div xmlns=…>`. Text escapes `"` as HAPI does, and an empty element
   is written as `<br/>`. HAPI-written XML is reproduced byte for byte, and other writers' XML at L2. A narrative of
   a contained resource is rewritten as well, but it is never stored.
7. *Validation the HAPI route did not do:* an element in a namespace other than FHIR's (outside a narrative), and a
   document type declaration, fail the read. Attributes in a namespace, such as `xsi:schemaLocation`, are ignored
   without a finding. Text inside a FHIR element is reported, once per path, in the read's warning.
8. `legacy-export` still writes through HAPI. It is the legacy route under FR-051 and outside this ticket.

### The owner's answers, and the rework (2026-10-22)

The owner reviewed the items of the comment above, under one principle: no code or tests that are not strictly
needed. Under `trust` that means ignoring silently, and failing only where carrying on would be unsafe or would
leave what the data is ambiguous.

1. **XML stores a decimal's text unchanged.** The marks, their regular-expression pass and the evidence for them
   are gone (`evidence/ticket-32/` marks the latter superseded). A decimal is stored as the text the XML carries,
   and integers are converted from the definitions. The decimal converter accepts text, which only XML gives it;
   JSON carrying a decimal as a string does not conform, which `trust` leaves undefined. XML egress writes a
   decimal as the text it is stored as, so the XML round trip keeps it end to end. `XmlJsonParityTest` and
   `writesWhatWasStored` compare decimals by value, at a double's precision, and everything else exactly.
   `storesADecimalAsItsText` pins `1.50`, `5`, `0.0008823084504462283`, `9999999999999999999` and `1.2E3`. The
   19-digit gap went with the marks. Notes were added under decision 134, against FR-060, and in tickets 16 and
   29.
2. **An invalid integer or boolean is null per value, with no finding.** This is accepted. The difference from
   JSON is listed in ticket 26's follow-up on non-conformant data. No test pinned it.
3. **The public sink and source** are ticket 39, in stage 5. The `io`-level sink and source stay here.
4. **Woodstox stays excluded.** Ticket 26 lists the check for the server.
5. **A `urn:` reference inside a primitive's extension** is a criterion of ticket 17.
6. **The narrative** is accepted as built.
7. **The read fails, naming the document, only** on text after the root or a second root, a DTD, and a root
   outside the FHIR namespace. Not well-formed XML fails too, as level 1. Everything else is ignored silently: an
   element in another namespace is dropped before the transform (`leavesOutAnElementInAnotherNamespace` pins that
   `<v:status>` is not read as `status`), a `div` that is not XHTML is dropped, namespaced attributes and text are
   ignored, and content the definitions do not describe is left out without a finding, `contained` aside, which
   is still reported (FR-006). `evidence/ingress-strictness/README.md` gains the XML column of the conformance
   levels and the XML rows of the table of what handles each level.
8. **`legacy-export`:** no action.
9. **SC-011** is accepted as drift and ticked. Ticket 10 gains the in-session measurement of XML lines against
   JSON. Without the marks' pass, XML lines at `l`, extensions off, one run of the phase harness, give: fit 295,
   1,339, 2,778 and 3,914 ms; encode 437, 1,991, 4,562 and 5,215 ms; decode 362, 1,506, 2,954 and 3,382 ms. The
   types are Patient, Condition, Encounter and Observation, against the bar's ext-0 fit 292, 2,362, 4,846 and 5,687
   ms. Observation's fidelity rises from 421/500 to 500/500, because the decimal's text is kept. The harness was
   not re-run in full.

**Removed under the principle:** the marks and their regex; the text-content findings and the passing on of
undescribed elements and attributes for reporting (XML no longer reports undescribed content, JSON still does,
per column); the failures on an element in another namespace and on a narrative that is not XHTML; the
findings parameter of the struct form of `toLayout`; the plain-form conversion of decimals on egress. These tests
were removed: `storesEmptyValuesAsJsonDoes`, `typesAColumnOfNumbersAsJsonWould`,
`failsOnAnElementInAnotherNamespace`, `failsOnAMalformedDocumentOfAnotherType`, `failsOnADocumentThatIsNotXml`
(a duplicate of `namesADocumentThatIsNotXmlByItsStart`), `failsOnADocumentInAnotherNamespace` (a duplicate of
the no-namespace case) and `failsOnANarrativeThatIsNotXhtml`.

**Kept, for the owner to confirm:** a document given to `readBundles` that is not a bundle still fails, in both
formats, as it did on the HAPI route. It is not one of item 7's three, but carrying on would return nothing
without saying why. On egress, a stored narrative that is not well-formed XHTML, and a line break the
line-delimited sink cannot escape, still fail, because carrying on would write a document that is not XML or
that spans lines.

**Tests after the rework:** `fhir-schema` 41 run, 0 failed, 0 skipped; `io` 844, 0, 1; `fhirpath` 7393, 0, 922;
`library-api` 326, 0, 0. Then the Ivy cache was cleared (`cache` and `jars` of `~/.ivy2` and `~/.ivy2.5.2` moved
aside and recreated): Python 572 passed and R 211 expectations passed, on the rebuilt jar (SHA-256 `d83b5a8f…`,
equal to the one Ivy resolved). `encoders`, `terminology` and `utilities` are unchanged and were not run.

### The owner's confirmation of the rework (owner, 2026-10-22)

Both items the rework left for the owner are accepted:

- The decimal converter accepts text, so a JSON decimal that arrives as a string is stored as that text rather than nulled with a finding. That input is not conformant, and `trust` leaves it undefined.
- The three failures kept outside item 7's list stay: `readBundles` on a document that is not a bundle, egress of a narrative that is not well-formed XHTML, and a line break the line-delimited sink cannot escape.

Performance is not pursued in this ticket. The single run after the rework, against the first run's minimums, put encode 13–21% higher on every type, and Patient's encode at 437 ms against the bar's 322 ms. That run is one sample against a minimum of five, so it may be noise. Ticket 10 measures it in stage 5.
