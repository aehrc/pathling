# 32 — XML and bundles without HAPI, on the interim ingest

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-045, FR-060, FR-061, FR-062, SC-011. Decision 134; decisions 83 and 84. [ADR-0004](../../../docs/adr/0004-no-per-row-hapi-no-catalyst-in-encoding.md).

**What to build:** FHIR XML is read and written without HAPI, and so are bundles in both formats. This is the interim of decision 134: JSON stays on Spark's reader and the generated transform, which is A0, and the losses OQ-1 lists are accepted. The aim is to remove the dependency on HAPI, not to change what is stored.

- **XML in.** One document per string, parsed with `from_xml`. Presence and fit come from Spark's XML inference with `inferSchema=false`. Types, cardinality and field order come from the definitions, and a transform generated from them shapes the result to the layout. This replaces `XmlConversion`, which today parses each document with HAPI and writes it out as JSON.
- **XML out.** The stored layout is shaped back into an XML struct and written with `to_xml`. This replaces the HAPI writer in `FhirXmlWriter`.
- **Bundles in.** Each bundle is exploded to the resources of the requested type, and its `urn:` references to other entries are resolved as decision 83 states, with no HAPI parse per bundle. This applies to XML and JSON bundles alike, and replaces `BundleTransformer`.
- **HAPI leaves ingest and egress.** `FhirParsers` and its reflection workaround, which finds custom resource types on an executor-side `FhirContext`, are removed. The FR-050 exception that `plan.md` records for XML and bundles is withdrawn.

**Interim behaviour, as decision 134 sets it.** The XML route stores what the JSON route stores, value for value: a decimal passes through a double, empty values are dropped as the JSON route drops them, and validation is by column type, reported once per column. It does not take on decision 133's per-value scope. Primitive ids and extensions are stored as the JSON route stores them until ticket 17.

**Starting points.**

- `evidence/xml-native/xml-native.md` establishes the route: row selection, inference, the transform, the narrative, bundles and writing. Its scripts are in `evidence/xml-native/scripts/`.
- `evidence/spike-08/harness/fromxml_route.py` is the Python prototype phase 1 measured, for all four benchmark types, lines and both bundle shapes, in and out. `gate_fromxml.py` is its correctness gate against HEAD. Both generate their expressions in Python over py4j, which is throwaway; the production transform is Java in `io`, built only from public Spark SQL API (FR-049).
- `evidence/spike-08/README.md`, phase 1, has its timings and the fixtures it was run against (`fixtures/fixtures-xml.tsv`). Against HEAD's HAPI route on the large types, it fitted lines at 0.50 to 0.61, encoded at 0.67 to 0.99 and decoded at 0.50 to 0.65.
- Phase 2's A+ XML route closed the document gaps below with a StAX pass per document (`evidence/spike-08/phase2/proto/src/au/csiro/pathling/io/Spike08XmlDocCheck.java`).

**Gaps phase 1 found in the prototype, to close here.**

- A document with a prefixed FHIR namespace, which is valid FHIR, is left out silently, because rows are selected by the root's literal name.
- Text after the root element is ignored, and a second root is dropped, silently. A document with no namespace is accepted. Each of these is malformed and fails the read (FR-063).
- The narrative is taken from the raw document by regular expression, and xml-native.md lists the cases that defeat it: a contained narrative, a comment before `</text>`, and CDATA. Use its StAX pass (option 3b), or report the cases it cannot handle. A narrative is never stored wrong silently.
- The bundle fit infers the whole bundle once per requested type, a constant of about 7.5 s per type at size `l`. That makes small types slower to fit than HAPI. One inference shared by all types removes it, and is a change to how bundles are read per type.

**Out of scope:** the per-value checks, findings and policies of decision 133, and keeping the decimal's text. These are ticket 16's, and wait for the choice decision 134 defers.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] XML lines are read through `from_xml` and a transform generated from the definitions. For every resource type of the benchmark corpus and the R4 examples, the stored schema and rows equal the JSON route's for the same resources (FR-060).
- [ ] XML is written through `to_xml`. Round-trip fidelity on the benchmark corpus is at least HEAD's.
- [ ] XML and JSON bundles, in the collection and transaction shapes, are exploded without HAPI. References resolve as decision 83 states, keeping the target's version (FR-062). An entry whose `fullUrl` is `urn:uuid:X` and whose resource `id` is `X` keeps its id, and a reference to it becomes `Type/X`; the HAPI route loses both (xml-native.md, "A defect found in the current route").
- [ ] A prefixed FHIR namespace is read. A missing namespace, text after the root and a second root fail the read, naming the document.
- [ ] A narrative is stored and written back byte for byte, or the document is reported. There is a test for each case xml-native.md lists.
- [ ] A custom resource type, such as `ViewDefinition`, is read and written in XML, single and in a bundle, through the definitions alone.
- [ ] No ingest or egress route imports HAPI's parser. `FhirParsers` and the reflection workaround are gone, and `CustomResourceTypeTest`'s guard on it is removed or replaced.
- [ ] Encode and decode of XML lines and bundles are no slower than the bar on the pathling-compare harness at size `l` (SC-011), or the owner accepts the gap.
- [ ] `plan.md`'s FR-050 exception for XML and bundles, and decision 84's HAPI routing, are amended to say they are withdrawn.

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
