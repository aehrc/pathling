---
sidebar_position: 2
description: Pathling can evaluate the terminology functions against a local terminology store built from SNOMED CT and FHIR terminology content, with no network dependency.
---

# Local terminology mode

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

By default the terminology functions call a remote
[FHIR terminology server](./server.md). As an alternative, Pathling can
evaluate the same functions against a **local terminology store** with no network dependency. You import SNOMED CT and FHIR
terminology content into the store once, then configure a context for local
mode pointing at that store. All seven terminology functions (`member_of`,
`translate`, `subsumes`, `subsumed_by`, `display`, `property_of` and
`designation`) work identically in local mode.

Local mode is useful when a terminology server is unavailable, when network
access or request volume is a constraint, or when reproducibility across
environments matters.

## Importing content

SNOMED CT is imported from an RF2 snapshot release (a `.zip` archive or an
extracted directory). FHIR CodeSystem, ValueSet and ConceptMap resources are
imported from a JSON file, a directory of JSON files, or a FHIR NPM package
(`.tgz`). The store is written as Delta tables under a location on any
filesystem accessible through the Hadoop FileSystem API, and can be reused
across sessions and from cluster deployments. Re-importing a version replaces it
atomically.

<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
pc.import_snomed(
    "/data/SnomedCT_InternationalRF2_PRODUCTION_20250601T120000Z.zip",
    "/data/tx-store",
)
pc.import_fhir_terminology("/data/hl7.terminology.r4-6.5.0.tgz", "/data/tx-store")
```

</TabItem>
<TabItem value="r" label="R">

```r
pc <- pathling_connect()
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store")
pathling_import_fhir_terminology(pc, "/data/hl7.terminology.tgz", "/data/tx-store")
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling import-snomed /data/rf2.zip /data/tx-store
pathling import-fhir-terminology /data/hl7.terminology.tgz /data/tx-store
```

</TabItem>
</Tabs>

### RF2 sources must be self-contained

An RF2 source is imported on its own terms: the concepts it ships are the
dictionary that every other file is resolved against. A description,
relationship or reference set row referencing a concept the source does not ship
has nothing to attach to, so it is dropped. A relationship needs both its source
and its destination concept, so one missing destination drops the row.

This is the ordinary shape of a **derived** or **extension** package. Such a
package declares its dependency on another edition through the Module Dependency
reference set and ships only its own modules' components, so most of what it
references lives in the edition it extends. Imported alone, it succeeds while
carrying almost none of the content you expected. Two published examples: the
SNOMED CT International Patient Summary ships no concepts of its own at all, and
the SNOMED CT Netherlands Patient Friendly Extension ships two bookkeeping
concepts alongside 1,287 active descriptions, of which 7 resolve.

To tell whether this has happened, read the per-file resolution counts in the
import log. Every file resolved against the concept dictionary reports one line:

```text
.../sct2_Description_Snapshot-en_NL_20200930.txt: 7 of 1287 active rows resolved against the concept dictionary.
```

A line is reported for every such file, including the files that resolve
completely, so a file whose two figures are equal is never confused with one that
was absent. Both figures count active rows only, because rows excluded for being
inactive are excluded by design rather than for want of a concept. The concept
file itself, the language reference sets and the Module Dependency reference set
produce no line, since none of them is resolved against the concept dictionary.

Unresolved rows are reported informationally and never fail the import:
importing a package whose references are mostly external is a legitimate thing to
do if that is what you intend. The lines are logged at `INFO` by the importer, so
they appear wherever logging for `au.csiro.pathling` is enabled at that level.

### Combining a derived package with its dependency

To import a derived package's content in full, combine it with the release it
declares a dependency on and import the combination as a single source.

Three roles are single-valued, so their files must be **concatenated** into one
file each, keeping only the first file's header row:

- the concept file (`sct2_Concept_Snapshot_*`)
- the relationship file (`sct2_Relationship_Snapshot_*`)
- the Module Dependency reference set (`der2_ssRefset_ModuleDependencySnapshot_*`)

Every other role is multi-valued, so those files are **left as they are**, each
keeping its own header, in the directory layout the import expects:

- descriptions and text definitions (`sct2_Description_*`, `sct2_TextDefinition_*`)
- language reference sets (`der2_cRefset_Language*`)
- all other reference sets (`der2_*Refset*`)

```bash
INT=/data/SnomedCT_InternationalRF2_PRODUCTION_20250601T120000Z/Snapshot
EXT=/data/SnomedCT_ExtensionRF2_PRODUCTION_20250930T120000Z/Snapshot
OUT=/data/merged/Snapshot

mkdir -p "$OUT/Terminology" "$OUT/Refset/Language" "$OUT/Refset/Content" \
         "$OUT/Refset/Metadata"

# Single-valued roles: concatenate, keeping only the first file's header row.
concat() {
  cat "$1" > "$3"
  tail -n +2 "$2" >> "$3"
}
concat "$INT"/Terminology/sct2_Concept_Snapshot_*.txt \
       "$EXT"/Terminology/sct2_Concept_Snapshot_*.txt \
       "$OUT/Terminology/sct2_Concept_Snapshot_MERGED.txt"
concat "$INT"/Terminology/sct2_Relationship_Snapshot_*.txt \
       "$EXT"/Terminology/sct2_Relationship_Snapshot_*.txt \
       "$OUT/Terminology/sct2_Relationship_Snapshot_MERGED.txt"
concat "$INT"/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_*.txt \
       "$EXT"/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_*.txt \
       "$OUT/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_MERGED.txt"

# Multi-valued roles: copy every file as it is. The two releases name their files
# by edition and date, so nothing is overwritten.
cp "$INT"/Terminology/sct2_Description_*.txt \
   "$INT"/Terminology/sct2_TextDefinition_*.txt \
   "$EXT"/Terminology/sct2_Description_*.txt "$OUT/Terminology/"
cp "$INT"/Refset/Language/*.txt "$EXT"/Refset/Language/*.txt "$OUT/Refset/Language/"
cp "$INT"/Refset/Content/*.txt "$EXT"/Refset/Content/*.txt "$OUT/Refset/Content/"

pathling import-snomed /data/merged /data/tx-store
```

Adjust the copies for the roles a given package actually ships; an extension
without text definitions, for instance, contributes none.

Do not simply extract both releases side by side into one directory. The import
rejects a source in which more than one file fills a single-valued role, naming
the role and every candidate path, because it cannot tell which tree's content
you meant and would otherwise proceed against one and silently ignore the other.

Combine a package only with the release it declares a dependency on. An
extension ships only its own modules' components, so a package and its
dependency do not overlap. Two overlapping editions do: the same concept code
would arrive twice, be given two internal identifiers, and fan out every join
built on it. Nothing detects that, so it is yours to avoid.

### Reducing the memory the hierarchy takes at query time

The largest structure a local store loads into memory is the hierarchy index,
which holds the transitive closure of the is-a graph as compressed bitmaps
addressed by an internal identifier per concept. By default those identifiers are
assigned in concept code order, and a code's numeric value bears no relation to
the concept's place in the hierarchy, so a concept's descendants scatter across
the whole identifier range and compress poorly.

The `pre-order` setting instead assigns identifiers by a depth-first traversal of
the is-a hierarchy, so each subtree occupies a near-contiguous interval. Measured
over a full SNOMED CT UK edition of 1,115,237 concepts, this reduces the
hierarchy index from 738 MB to 536 MB of retained heap, a saving of 27%.

The trade-off is identifier stability. Under the default ordering a concept keeps
its identifier across re-imports of any release that contains it, and identifiers
change only where codes are added or removed. Under the pre-order, a change
anywhere in the shape of the hierarchy shifts the identifiers of everything that
follows it, so identifiers vary much more between releases. Identifiers are
internal to a store and never appear in query results, so this affects nothing a
user can observe directly; it matters only if you compare or reuse the internal
identifiers of two separately imported stores. Repeated imports of the same
release remain reproducible under both orderings, and all seven terminology
functions return identical results either way.

<Tabs>
<TabItem value="python" label="Python">

```python
pc.import_snomed(
    "/data/rf2.zip",
    "/data/tx-store",
    dense_id_order="pre-order",
)
```

</TabItem>
<TabItem value="r" label="R">

```r
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store",
  dense_id_order = "pre-order")
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling import-snomed /data/rf2.zip /data/tx-store --dense-id-order pre-order
```

</TabItem>
</Tabs>

### Large CodeSystems

CodeSystems are imported with bounded memory regardless of their size. The
import streams each CodeSystem from the source, transcodes it to temporary files
on the driver, and loads it with Spark, so peak memory does not grow with the
number of concepts and a single CodeSystem may exceed the 2 GB limit on a single
in-memory object (for example, the OMOP vocabulary package's multi-gigabyte
CodeSystem). This applies equally to a bare JSON file, a directory, and a `.tgz`
package.

Peak memory is bounded, but the largest vocabularies still need more than the
default 1 GB driver heap to hold the working set of the Spark joins that build
the store; the OMOP vocabulary, for example, imports comfortably with a 4 GB
heap. See
[driver memory for large imports](../cli#terminology-import-commands) in the CLI
guide for how to raise it, which applies equally to imports run through the
library.

During a long import, a running count of parsed concepts is logged at a fixed
interval alongside stage-transition messages, so progress is visible rather than
appearing to hang. The CLI surfaces these messages in `--verbose` mode.

### Hierarchies from parent and child properties

Many flat CodeSystems express their hierarchy through `parent` (or `child`)
concept properties rather than nested concepts. The import derives is-a edges
from code-valued `parent` and `child` properties, recognised by the standard
`parent`/`child` property codes or a property declaration carrying the standard
[concept-properties](https://hl7.org/fhir/codesystem-concept-properties.html)
URI, in addition to concept nesting. Edges from both sources are combined, so
subsumption and descendant-based membership queries work over property-based
hierarchies just as they do over nested ones. A `parent` or `child` reference to
a code absent from the CodeSystem is skipped with a warning, and duplicate
concept codes resolve to their first occurrence with a warning.

### Bundles and non-CodeSystem resources

ValueSets and ConceptMaps are stored whole, so a single resource must fit in
memory; one larger than 1 GB fails with an actionable error naming the resource
rather than an opaque memory error. Bundle-wrapped sources are also parsed in
memory, so a Bundle is subject to the same in-memory limit; supply large
CodeSystems as standalone resources to benefit from the streaming path.

### Recovering from a failed import

If an import fails partway through writing a CodeSystem (for example, because the
source is truncated or corrupt), it reports that the store may hold a partial
version of that CodeSystem and advises re-running the import. Because content is
keyed by system version, re-running with a corrected source fully replaces the
partial version and repairs the store.

## Querying in local mode

Create a context configured for local mode by setting the terminology mode to
`local` and pointing at the store. The terminology functions then evaluate
against the store.

<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext
from pathling.functions import to_snomed_coding
from pathling.udfs import member_of
from pyspark.sql import functions as F

pc = PathlingContext.create(
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
)

result = df.select(
    "id",
    member_of(
        to_snomed_coding(F.col("code")),
        "http://snomed.info/sct?fhir_vs=ecl/<< 73211009 |Diabetes mellitus|",
    ).alias("is_diabetes"),
)
```

</TabItem>
<TabItem value="r" label="R">

```r
pc <- pathling_connect(
  terminology_mode = "local",
  terminology_storage_path = "/data/tx-store"
)
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling --tx-store /data/tx-store member-of codes.csv \
  --code-column code --system 'http://snomed.info/sct' \
  --value-set 'http://snomed.info/sct?fhir_vs=ecl/<< 73211009'
```

The store can also be recorded once in the `[tx-store]` config table. See the
[command line interface documentation](../cli#local-terminology-mode) for details.

</TabItem>
</Tabs>

The following configuration parameters control local mode:

- `terminology_mode` (`terminology.mode`): `server` (the default) or `local`.
- `terminology_storage_path` (`terminology.local.storagePath`): the store
  location, required in local mode.
- `default_snomed_edition` (`terminology.local.defaultSnomedEdition`): the
  SNOMED CT module identifier used to disambiguate an unversioned SNOMED
  reference when the store holds multiple editions.
- `expansion_cache_size` (`terminology.local.expansionCacheSize`): the maximum
  number of value set expansions cached per executor.
- `dialect_aliases` (`terminology.local.dialectAliases`): additional dialect
  tags recognised when a display is requested in a particular language. See
  [dialects](#dialects).

## Dialects

Within SNOMED CT, which of a concept's synonyms is its _preferred term_ is not a
property of the concept but of a **language reference set**. Two reference sets
of the same language routinely disagree: the International edition ships both GB
English and US English, and `32849002` is "Oesophageal structure" in the first
and "Esophageal structure" in the second. A **dialect** is the caller-facing name
for one of those reference sets.

### Naming a dialect

A dialect may be named in any of three ways, and all three are interchangeable:

| Form                       | Example                             | Notes                                                                                                                                       |
| -------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| A recognised tag           | `en-GB`                             | Matched without regard to case. The recognised tags are below.                                                                              |
| An extension tag           | `en-x-sctlang-90000000-00005080-04` | The form Pathling reports as the language of a preferred designation, so a language reported on the way out can be requested on the way in. |
| A reference set identifier | `900000000000508004`                | Accepted by the import option only, not by a query-time language request.                                                                   |

The following tags are recognised out of the box. They cover the language
reference sets defined in the SNOMED CT International edition; a reference set
defined inside a national extension is reached through an alias or through the
extension tag form.

| Tag     | Language reference set |
| ------- | ---------------------- |
| `en-GB` | `900000000000508004`   |
| `en-US` | `900000000000509007`   |
| `en-AU` | `32570271000036106`    |
| `es`    | `448879004`            |
| `fr`    | `722131000`            |
| `de`    | `722130004`            |
| `ja`    | `722129009`            |
| `zh`    | `722128001`            |

A tag naming no reference set - a bare `en`, or a region nothing covers -
expresses no preference rather than an error, and the stored display answers.

### Requesting a term in a dialect

The `accept_language` context parameter, and the parameter of the same name on
`display()` and `property_of()`, select by dialect:

```python
pc = PathlingContext.create(
    spark,
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
)

# "Oesophageal structure", the term GB English prefers.
british = property_of(coding, "display", accept_language="en-GB")

# "Esophageal structure", the term US English prefers.
american = property_of(coding, "display", accept_language="en-US")
```

A weighted list is read as RFC 9110 describes, and each dialect is tried in
descending order of weight until one yields a term. With
`accept_language="en-NZ;q=0.9,en-GB;q=0.5"` against a store holding no New
Zealand reference set, the GB English term answers. A tag given zero weight is
never used, and a lone `*` expresses no preference.

### The default dialect of a store

Every concept in the store carries one **stored display**, which is what a
request naming no dialect - or naming one the store cannot serve - receives. That
display is the preferred synonym of a single dialect, chosen when the release is
imported:

1. The dialect named by the `default_dialect` import option, if one is given.
2. The sole language reference set, where the release holds only one.
3. US English, where the release is the SNOMED CT International edition.

A release that holds several language reference sets and is not the International
edition **fails the import**, listing every candidate by identifier and by the
name the release itself gives it, so that one can be named:

```text
The release holds 3 language reference sets and none of them is a clear default. Name one with the defaultDialect import option:
  900000000000508004  Great Britain English language reference set
  999000691000001104  National Health Service realm language reference set (pharmacy part)
  999001261000000100  NHS realm language reference set (clinical part)
```

No SNOMED CT release declares which of its language reference sets is the
default, so where the release is genuinely ambiguous the choice is the
operator's rather than a guess. Nothing is written to the store when the import
fails this way.

<Tabs>
<TabItem value="python" label="Python">

```python
pc.import_snomed("/data/rf2.zip", "/data/tx-store", default_dialect="en-GB")
```

</TabItem>
<TabItem value="r" label="R">

```r
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store", default_dialect = "en-GB")
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling import-snomed --default-dialect en-GB /data/rf2.zip /data/tx-store
```

The dialect can also be recorded once as the `tx-store.default-dialect` config
key, which applies whenever the flag is omitted - see the
[command line interface documentation](../cli#terminology-import-commands).

</TabItem>
</Tabs>

Where the chosen dialect marks no preferred synonym for a concept, its display
falls to the preferred synonym of the lowest-numbered other language reference
set, then to its fully specified name, and finally to its own code.

### Registering additional dialects

A deployment can register its own dialect tags, which is how a reference set
defined inside a national extension is reached by a familiar name. An entry for a
tag that is already recognised replaces the built-in mapping, so a built-in entry
can be corrected.

<Tabs>
<TabItem value="python" label="Python">

```python
pc = PathlingContext.create(
    spark,
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
    dialect_aliases={"en-NZ": "271000210107"},
)
```

</TabItem>
<TabItem value="r" label="R">

```r
pc <- pathling_connect(
  terminology_mode = "local",
  terminology_storage_path = "/data/tx-store",
  dialect_aliases = c("en-NZ" = "271000210107")
)
```

</TabItem>
<TabItem value="cli" label="CLI">

```toml
[tx-store]
path = "/data/tx-store"

[tx-store.dialect-aliases]
en-NZ = "271000210107"
```

</TabItem>
</Tabs>

Aliases affect the selection of a display only. The designations of a concept
are returned regardless of the language requested, and a synonym preferred
within a language reference set is labelled with the extension tag form rather
than an alias - see
[multi-language support](./index.md#multi-language-support). Aliases are not
consulted by an import, which receives no service configuration; a reference set
outside the recognised tags is named there by its identifier.

The R binding can carry at most ten aliases, a limit of how sparklyr passes a map
to the JVM. The Java, Python and command line surfaces have no such limit.

### Code systems that are not SNOMED CT

A FHIR CodeSystem carries plain BCP-47 designation languages with no reference
set to resolve, so there a language request is matched against the designation
languages directly. A designation whose tag matches the request exactly is
preferred over one matching only on the primary subtag, and within each of those
one whose use is `display` is preferred. An extension tag has no meaning outside
SNOMED CT, so it falls back to its plain language subtag.

## Value set and concept map expressions

Local `member_of` resolves three kinds of value set reference, and local
`translate` two kinds of concept map reference. A reference that resolves to
content the store does not hold yields the same "unknown content" results as
remote mode rather than an error.

| Reference                             | Form                                               |
| ------------------------------------- | -------------------------------------------------- |
| An imported FHIR ValueSet             | its canonical URL, optionally with `\|version`     |
| Every SNOMED CT concept               | `http://snomed.info/sct?fhir_vs`                   |
| A SNOMED CT reference set             | `http://snomed.info/sct?fhir_vs=refset/[refsetId]` |
| A SNOMED CT subtype hierarchy         | `http://snomed.info/sct?fhir_vs=isa/[conceptId]`   |
| A SNOMED CT ECL expression            | `http://snomed.info/sct?fhir_vs=ecl/[expression]`  |
| A VCL expression                      | `http://fhir.org/VCL?v1=[expression]`              |
| An imported FHIR ConceptMap           | its canonical URL                                  |
| A SNOMED CT association reference set | `http://snomed.info/sct?fhir_cm=[refsetId]`        |

The `isa/` form is the subtype hierarchy including the named concept itself, so
it is equivalent to `ecl/<<[conceptId]`. Every SNOMED form is also accepted on an
edition and version qualified URI, for example
`http://snomed.info/sct/32506021000036107/version/20250630?fhir_vs=ecl/...`,
which evaluates against that version rather than the store default. Any other
`fhir_vs` value is treated as unknown content.

The expression carried by an `ecl/` or `v1=` URL is percent-decoded before it is
parsed, so it must be percent-encoded when the URL is built. For ECL,
`to_ecl_value_set` does this (`tx_to_ecl_value_set` in R, `toEclValueSet` in Java
and Scala); there is no equivalent helper for VCL, so encode it yourself.

Both expression languages are evaluated by the same engine: an ECL expression is
translated into the VCL model, and the result is evaluated against the store's
indexes. Members are restricted to active concepts, matching the behaviour of a
terminology server on an implicit value set, unless the expression explicitly
asks for inactive concepts with an `inactive = true` filter.

### SNOMED CT Expression Constraint Language

The grammar recognises the whole of ECL v2, and the translator maps the subset
below. A construct outside the subset is rejected with an error naming it, so a
query is never quietly answered with the wrong members. A malformed expression
raises `Invalid ECL expression at position [n]: [reason]`.

| Construct                                 | Example                                                      |
| ----------------------------------------- | ------------------------------------------------------------ |
| Concept reference, with an optional term  | `73211009 \|Diabetes mellitus\|`                             |
| Wildcard                                  | `*`                                                          |
| Descendant or self, descendant            | `<< 73211009`, `< 73211009`                                  |
| Child of                                  | `<! 73211009`                                                |
| Ancestor or self, ancestor                | `>> 73211009`, `> 73211009`                                  |
| Parent of                                 | `>! 73211009`                                                |
| Reference set membership                  | `^ 447562003`                                                |
| Conjunction, disjunction, exclusion       | `<< 73211009 MINUS << 46635009`                              |
| Grouping                                  | `(< 19829001) OR (< 301867009)`                              |
| Attribute refinement                      | `< 404684003 : 363698007 = << 39057004`                      |
| Attribute negation                        | `< 404684003 : 363698007 != << 39057004`                     |
| Descendant attribute types                | `< 404684003 : << 47429007 = << 39057004`                    |
| Attribute sets, with `OR` binding loosest | `< 404684003 : 363698007 = 1, 246075003 = 2 OR 47429007 = 3` |
| Nested attribute sets                     | `< 404684003 : (363698007 = 1 OR 246075003 = 2)`             |
| Dotted attribute navigation               | `< 19829001 . 116676008`                                     |

A hierarchy operator applies to a concept reference only. An attribute name may
carry `<` or `<<`, which broadens the constraint to descendant attribute types.

The viral infection expression under
[value set membership](./index.md#value-set-membership) is within the subset: its
two attributes are separated by a comma rather than grouped into a role group.

Each rejected construct raises `Unsupported ECL construct: [construct]`, where
the construct is one of:

| Construct                                                                         | ECL                                   |
| --------------------------------------------------------------------------------- | ------------------------------------- |
| `grouped attributes ({ ... })`                                                    | `< 404684003 : { 363698007 = 1 }`     |
| `attribute cardinality ([min..max])`                                              | `< 404684003 : [1..2] 363698007 = 1`  |
| `reverse attribute flag (R)`                                                      | `< 404684003 : R 363698007 = 1`       |
| `wildcard attribute name`                                                         | `< 404684003 : * = 1`                 |
| `hierarchy operator other than < or << on an attribute name`                      | `< 404684003 : >> 363698007 = 1`      |
| `hierarchy operator on an attribute name`                                         | `< 19829001 . << 116676008`           |
| `child-or-self operator (<<!)`                                                    | `<<! 73211009`                        |
| `parent-or-self operator (>>!)`                                                   | `>>! 73211009`                        |
| `hierarchy operator applied to a wildcard, reference set, or compound expression` | `<< (73211009 OR 46635009)`           |
| `reference set membership over a wildcard or expression`                          | `^ *`                                 |
| `concrete value (#number or "string")`                                            | `< 404684003 : 1142135004 = #20`      |
| `term, definition status, or member filter ({{ ... }})`                           | `<< 73211009 {{ term = "diabetes" }}` |
| `history supplement ({{ + ... }})`                                                | `<< 73211009 {{ + HISTORY }}`         |

### FHIR ValueSet Compose Language

The VCL grammar is transcribed from the
[FHIR IG Guidance specification](https://build.fhir.org/ig/FHIR/ig-guidance/vcl.html),
and the whole of VCL v1 parses. A malformed expression raises
`Invalid VCL expression at position [n]: [reason]`.

An expression must name its code system with a `(systemUri)` scope, because a
value set is evaluated over one code system version. The first scope in the
expression selects that version, optionally by `|version`; a subexpression scoped
to any other system contributes no members. An expression carrying no scope at
all is treated as unknown content.

| Construct              | Example                                   | Meaning                                                    |
| ---------------------- | ----------------------------------------- | ---------------------------------------------------------- |
| System scope           | `(http://loinc.org\|2.74)4548-4`          | a code in a version of a system                            |
| Code                   | `4548-4`                                  | one concept                                                |
| Wildcard               | `*`                                       | every concept                                              |
| Conjunction            | `A,B`                                     | in both                                                    |
| Disjunction            | `A;B`                                     | in either                                                  |
| Exclusion              | `A - B`                                   | in A but not B                                             |
| Grouping               | `(http://loinc.org)(41995-2;4548-4)`      | a scope or operator over a group                           |
| Equality               | `concept = 73211009`                      | exactly that concept                                       |
| Is-a, is-not-a         | `concept << X`, `concept ~<< X`           | that subtype hierarchy, or every active concept outside it |
| Descendant, child      | `concept < X`, `concept <! X`             | strict descendants, direct children                        |
| Generalises            | `concept >> X`                            | ancestors of the concept, and itself                       |
| Descendant leaves      | `concept !!< X`                           | descendants that have no children                          |
| Regular expression     | `code / "A.*"`                            | a property value fully matching the pattern                |
| In, not in a code list | `concept ^ {A,B}`, `concept ~^ {A,B}`     | membership in an enumerated list                           |
| In a filter list       | `363698007 ^ {concept << X}`              | membership in a nested constraint                          |
| Exists                 | `ingredient ? true`                       | whether the property is present                            |
| Navigation             | `X.parent`, `{A,B}.parent`, `*.116676008` | the values a property takes                                |

The properties a filter may name are:

- `concept`, which applies the hierarchy operators to the subsumption graph. For
  a code system that is not SNOMED CT that graph comes from the CodeSystem's
  nesting and its `parent` and `child` properties, as described under
  [hierarchies from parent and child properties](#hierarchies-from-parent-and-child-properties).
- `parent` and `child`, which take one edge rather than the closure, so
  `parent = X` selects the children of X and `child = X` selects its parents.
- `inactive`, `moduleId`, `sufficientlyDefined` and `effectiveTime`, which are
  the SNOMED CT concept metadata held in the store.
- Any property declared by an imported FHIR CodeSystem, over which `=`, `/`,
  `^`, `~^` and `?` behave as described above and any other operator falls back
  to an exact match.
- Any other property, which is read as a SNOMED CT attribute type, so
  `363698007 = 39057004` selects the concepts having that attribute with that
  value.

Three limitations are worth knowing:

- A value set inclusion, whether `^http://example.org/ValueSet/x` at the top
  level or `property ^ http://example.org/ValueSet/x` as a filter value, parses
  but contributes no members. Enumerate the codes or nest a filter list instead.
- An attribute filter matches the value named, and does not apply its operator to
  that value: `363698007 << 39057004` is the same as `363698007 = 39057004`. To
  match the value's subtree, nest the constraint as
  `363698007 ^ {concept << 39057004}`, or write the whole thing in ECL, where
  `363698007 = << 39057004` does expand the value.
- The exclusion operator must be surrounded by whitespace, because a hyphen is a
  valid character within a code: `A - B` excludes B from A, while `A-B` is the
  single code `A-B`. For the same reason a code containing a full stop must be
  quoted, otherwise it parses as property navigation: `"B.123"`, not `B.123`.

This example selects diabetes mellitus and its subtypes, less type 1 diabetes
and its subtypes, over the store's default SNOMED CT edition:

<Tabs>
<TabItem value="python" label="Python">

```python
from urllib.parse import quote

VCL = "(http://snomed.info/sct)concept << 73211009 - concept << 46635009"
value_set = "http://fhir.org/VCL?v1=" + quote(VCL, safe="")

result = df.select("id", member_of(to_snomed_coding(F.col("code")), value_set))
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling --tx-store /data/tx-store member-of codes.csv \
  --code-column code --system 'http://snomed.info/sct' \
  --value-set 'http://fhir.org/VCL?v1=(http://snomed.info/sct)concept%20%3C%3C%2073211009%20-%20concept%20%3C%3C%2046635009'
```

</TabItem>
</Tabs>
