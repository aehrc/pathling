---
slug: local-terminology
title: Terminology without a terminology server
authors: [johngrimes]
tags: [release, libraries, terminology]
---

Pathling 9.9.0 adds a local terminology mode. SNOMED CT and FHIR terminology
content is imported once into a store on your own filesystem, and the
terminology functions then evaluate against that store instead of calling a
remote FHIR terminology server. The same seven functions, `member_of`,
`translate`, `subsumes`, `subsumed_by`, `display`, `property_of` and
`designation`, work identically in both modes. What changes is what you can do
with them.

<!-- truncate -->

## The server that used to be in the loop

Until now, every terminology function in Pathling was a wrapper around a call to
a FHIR terminology server. `member_of` became `$validate-code`, `subsumes`
became `$subsumes`, `display` became `$lookup`, and so on. Responses were cached
on each executor, so a query did not make one request per row, but the server
was always there, and that had consequences.

- You needed a server you could reach. The default,
  `https://tx.ontoserver.csiro.au/fhir`, is suitable for testing only, so
  anything real meant running your own or licensing access to someone else's.
- Your analytics were coupled to a network service. A Spark job over a large
  dataset could be slowed, or fail, because of the terminology server's
  availability or request limits rather than anything in the data.
- Your results depended on what the server had loaded on the day. A value set
  expanded against one SNOMED CT release in January might have different
  members in July.
- Any environment with no outbound network access was ruled out entirely.

Local mode removes the server from that picture.

## What you can now do

### Run terminology queries where there is no network

Trusted research environments, hospital analytics platforms and other locked
down settings routinely block outbound connections. Previously, that meant
Pathling's terminology functions were unavailable there. Now the store is a
set of Delta tables under a path, so it travels with the data. Build it on a
machine that has the release files, copy it to S3, HDFS or a local disk
alongside your FHIR data, and query it with no network at all.

The command line interface can run an entire pipeline this way. Once a store
has been populated, `--tx-store` points any command that evaluates terminology
at it:

```bash
pathling import-snomed /data/SnomedCT_InternationalRF2_PRODUCTION_20250601T120000Z.zip /data/tx-store

pathling --tx-store /data/tx-store view /data/fhir --view diabetes_cohort.json --format csv
```

The same applies to `fhirpath`, `run`, `console` and the seven terminology
commands.

### Scale the query without scaling a server

In local mode, each Spark executor loads the store's indexes and answers
terminology questions itself. The transitive closure of the SNOMED CT is-a
hierarchy is held as compressed bitmaps, so a subsumption test or an ECL
descendant query is a bitmap operation rather than a round trip. Value set
expansions are cached per executor.

The largest of those indexes is the hierarchy. Over a full SNOMED CT UK edition
of 1,115,237 concepts it takes 738 MB of heap with the default identifier
ordering, or 536 MB with the `pre-order` import option, which assigns internal
identifiers by a depth-first walk of the hierarchy so that each subtree
compresses as a near-contiguous interval. Either way, the whole edition fits
comfortably on an ordinary executor, and there is no service on the other end
to become the bottleneck as the cluster grows.

### Pin the exact release your results depend on

A store holds specific versions of specific code systems. You decide when to
import a new release, and re-importing a version replaces it atomically. Every
SNOMED CT value set form also accepts an edition and version qualified URI, so
a query can name precisely the content it was validated against:

```python
member_of(
    to_snomed_coding(F.col("code")),
    "http://snomed.info/sct/32506021000036107/version/20250630?fhir_vs=ecl/<< 73211009",
)
```

Because the store is just files, it can be versioned, archived and shipped with
a study. A cohort definition run against the same store in two environments
returns the same members, which was not something the remote mode could
promise.

### Bring your own content without standing up a server

FHIR CodeSystem, ValueSet and ConceptMap resources can be imported from a JSON
file, a directory of JSON files, or a FHIR NPM package. Loading a local code
system, a set of curated value sets or a mapping table used to require a
terminology server to load them into. Now it is one call:

```python
pc.import_fhir_terminology("/data/hl7.terminology.r4-6.5.0.tgz", "/data/tx-store")
pc.import_fhir_terminology("/data/our-local-codes/", "/data/tx-store")
```

CodeSystems are streamed with bounded memory, so a single resource larger than
the 2 GB limit on an in-memory object imports without difficulty. The OMOP
vocabulary package, which ships a multi-gigabyte CodeSystem, imports with a
4 GB driver heap. Hierarchies expressed through `parent` and `child` properties
are recognised, so `subsumes` and descendant based `member_of` work over flat
code systems as they do over SNOMED CT. Imported ConceptMaps drive `translate`
in both directions.

### Choose the dialect

Which synonym of a SNOMED CT concept is preferred is a property of a language
reference set, not of the concept. Local mode makes that explicit. A store has
a default dialect, chosen at import time, and any query can ask for another:

```python
# "Oesophageal structure".
property_of(coding, "display", accept_language="en-GB")

# "Esophageal structure".
property_of(coding, "display", accept_language="en-US")
```

`en-GB`, `en-US`, `en-AU`, `es`, `fr`, `de`, `ja` and `zh` are recognised out
of the box, weighted preference lists such as `en-NZ;q=0.9,en-GB;q=0.5` are
honoured, and a deployment can register aliases for the language reference sets
of a national extension.

### Write value sets in ECL or VCL and evaluate them locally

`member_of` resolves imported ValueSets by canonical URL, the SNOMED CT implicit
forms (`?fhir_vs`, `refset/`, `isa/` and `ecl/`), and
[VCL](https://build.fhir.org/ig/FHIR/ig-guidance/vcl.html) implicit value sets
of the form `http://fhir.org/VCL?v1=...`. Both expression languages run on the
same engine against the store's indexes. This ECL expression, for instance,
selects diabetes mellitus and its subtypes less type 1 diabetes and its
subtypes:

```text
<< 73211009 |Diabetes mellitus| MINUS << 46635009 |Type 1 diabetes mellitus|
```

The ECL translator covers hierarchy operators, reference set membership,
conjunction, disjunction and exclusion, attribute refinement and dotted
attribute navigation. A construct outside that subset, such as a role group or
a cardinality constraint, is rejected with an error naming it rather than
answered with the wrong members.

## What stays the same

Switching modes is a configuration change. The functions, their signatures and
their results are the same, and any existing code that uses them runs
unchanged:

```python
from pathling import PathlingContext

pc = PathlingContext.create(
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
)
```

Remote mode remains the default and is unchanged. Local mode is available
through the Python and R libraries, the Java and Scala API and the command line
interface.

## Things worth knowing

- You need the release files. SNOMED CT is imported from an RF2 snapshot,
  obtained under licence from your national release centre or from SNOMED
  International. Pathling does not distribute terminology content.
- An RF2 source is imported on its own terms. A derived package or national
  extension that depends on another edition must be combined with that edition
  before import, or most of its content will have nothing to attach to. The
  documentation describes how, and the import log reports how much of each
  file resolved.
- The ECL subset excludes grouped attributes, cardinality, the reverse flag,
  concrete values, term filters and history supplements.

## Getting started

```bash
pip install pathling
```

Import a release, create a context in local mode and query as before. The
[local terminology mode documentation](/docs/libraries/terminology/local)
covers the import commands, dialects, and the ECL and VCL forms in full, and
the [command line interface](/docs/libraries/cli#local-terminology-mode) guide
covers offline use from the shell. The full list of changes in 9.9.0 is in the
[release notes](https://github.com/aehrc/pathling/releases/tag/v9.9.0).
