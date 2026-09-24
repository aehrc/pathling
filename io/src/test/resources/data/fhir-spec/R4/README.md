# FHIR R4 specification examples

A curated subset of the example resources published with the FHIR R4
specification, used by `SpecExampleRoundTripTest` to prove FR-016 and SC-001
over a corpus that is not machine-generated.

## Provenance

| | |
| --- | --- |
| Source | <https://hl7.org/fhir/R4/examples-json.zip> |
| FHIR version | R4, 4.0.1 |
| Retrieved | 2026-09-17 |
| Licence | CC0. The FHIR specification is published as a public domain dedication. |

The archive holds 2,912 files and 199 MB. This directory holds 161 of them,
541 KB, selected as described below.

## Why this corpus and not only Synthea

The two corpora fail differently, so `io` carries both.

Synthea is high volume and narrow variety: one generator, the same handful of
profiles repeated, no structural surprises. It proves the layout survives
realistic bulk data.

These examples are hand-authored by the specification's editors to demonstrate
each element, so they are small, broad, and deliberately awkward. They carry
contained resources, extensions nested three deep, every branch of a choice
type, dates at each precision, and decimals written in forms a generator never
emits. A round trip that passes Synthea and fails here is the ordinary
outcome — which is the point of running both.

## How the subset was selected

Two layers, and `MANIFEST.tsv` records which applied to each file.

1. **Every resource type once.** The smallest hand-authored example of each of
   the 140 non-`Bundle` types, preferring a real example over a generated
   `.profile.json` or `-questionnaire.json` where the type has one. This is
   what guarantees each derived schema is exercised at all, rather than only
   the types someone thought to check.
2. **Every structural corner once.** A greedy cover over 79 named corners —
   the decimal lexical forms, date and dateTime precisions, `base64Binary`,
   xhtml narrative, choice types, contained resources, extensions and
   modifier extensions, arrays of primitives and of complex elements, nesting
   depth, and non-ASCII text — taking the smallest file that covers a corner
   nothing already selected covers.

`json-edge-cases.json` and `parameters-example.json` are included by name.
They carry corners nothing else in the corpus does, and the specification
ships the first under that name for exactly this purpose.

## Packaging

The examples are published one pretty-printed resource per file; the harness
reads newline-delimited JSON, one file per resource type. The conversion
removes newlines and nothing else.

**No step in the packaging parses a value.** A literal newline cannot appear
inside a JSON string, so removing every newline leaves the bytes of every
value exactly as the specification wrote them. Anything that round-tripped the
document through a JSON parser would normalise `1.50` to `1.5` and re-render
`1e2` as `1E+2`, silently destroying the property this corpus exists to test.
The packaging verifies this by re-reading both forms with every number kept as
its source text and asserting they are equal.

## The one content change

`json-edge-cases.json` and `parameters-example.json` are the only two examples
in the archive without an `id`. The harness pairs the two sides of a round trip
by identifier, so each was given one — `json-edge-cases` and
`parameters-example` respectively, inserted as text immediately after
`resourceType`. `MANIFEST.tsv` flags both in its `assignedId` column. Nothing
else in any file was altered.

## Bundles

`Bundle.ndjson` is present and is never round-tripped. FR-007 means a bundle is
never stored as a resource type, so it cannot round-trip as one; its contents
round-trip as the resources it is exploded into, which is M3's concern. The
bundles are here so that the exclusion has something to act on and cannot pass
vacuously.

## Refreshing it

`.local/work/r4-examples/` holds the three scripts that produced this
directory — `curate.py` detects the corners, `select.py` covers them, and
`package.py` writes the NDJSON and verifies the collapse. They are scratch
rather than build inputs: the corpus is vendored so the build stays offline,
and regenerating it is a deliberate act, not a build step.
