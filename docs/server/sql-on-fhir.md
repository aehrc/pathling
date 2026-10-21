# SQL on FHIR

Pathling Server implements the [SQL on FHIR](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/) specification through two operations:

* [`$sql-run`](/docs/server/operations/sql-run.md) runs one subject synchronously and returns its result in the response body.
* [`$sql-export`](/docs/server/operations/sql-export.md) runs one or more subjects asynchronously and writes each result to downloadable files.

This page describes the behaviour both operations share: the kinds of subject, how a SQL subject is written, how it declares the tables it reads, and how it uses value sets and concept maps. The operation pages describe each operation's parameters, output formats and status codes.

## Subjects[​](#subjects "Direct link to Subjects")

A *subject* is the artefact an operation runs. There are three kinds:

| Kind                                                                                                    | Resource         | What it is                                                                 | Can be a dependency |
| ------------------------------------------------------------------------------------------------------- | ---------------- | -------------------------------------------------------------------------- | ------------------- |
| [ViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html) | `ViewDefinition` | Projects one resource type into a flat table, using FHIRPath.              | yes                 |
| [SQLQuery](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLQuery.html)             | `Library`        | Runs SQL over the tables it declares as dependencies. May take parameters. | no                  |
| [SQLView](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLView.html)               | `Library`        | A named, reusable SQL table source that other SQL artefacts can depend on. | yes                 |

Both operations name a subject in one of three ways, and exactly one must be supplied per subject:

* `subjectCanonical` - the canonical URL of a stored artefact, optionally pinned with `|version`. It is matched against both `ViewDefinition.url` and `Library.url`.
* `subjectReference` - a relative reference to a stored artefact, such as `ViewDefinition/conditions` or `Library/cvd-patient-count`.
* `subjectResource` - the artefact itself, inline in the request.

ViewDefinition and Library resources are stored with the ordinary [create and update](/docs/server/operations/crud.md) operations, for example `PUT [base]/Library/cvd-patient-count`.

## ViewDefinitions[​](#viewdefinitions "Direct link to ViewDefinitions")

A ViewDefinition selects one resource type and defines its columns with FHIRPath expressions. This one projects each coding of each Condition into a row, and is the `conditions` table used by the examples on this page:

```
{
    "resourceType": "ViewDefinition",
    "url": "https://example.org/ViewDefinition/conditions",
    "name": "conditions",
    "status": "active",
    "resource": "Condition",
    "select": [
        {
            "column": [
                { "name": "patient_id", "path": "subject.getReferenceKey()" }
            ]
        },
        {
            "forEach": "code.coding",
            "column": [
                { "name": "system", "path": "system" },
                { "name": "code", "path": "code" }
            ]
        }
    ]
}
```

The FHIRPath functions available within a ViewDefinition, including the terminology functions such as `memberOf()` and `translate()`, are described in the [FHIRPath documentation](/docs/fhirpath.md). A ViewDefinition subject can also be run over FHIR resources supplied with a `$sql-run` request instead of server data; see the `resource` parameter of [`$sql-run`](/docs/server/operations/sql-run.md#parameters).

## SQLQueries and SQLViews[​](#sqlqueries-and-sqlviews "Direct link to SQLQueries and SQLViews")

SQLQuery and SQLView are profiles of `Library`, and are written the same way:

* `type` carries a coding with system `https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes` and code `sql-query` or `sql-view`.
* `content` carries one attachment whose `contentType` is `application/sql` and whose `data` is the SQL text, Base64 encoded.
* `relatedArtifact` declares every table the SQL reads, as described under [Dependencies](#dependencies).
* `parameter` declares the runtime parameters of a SQLQuery. A SQLView must declare none.

This SQLView keeps the `conditions` rows whose code belongs to a value set (see [Value sets](#value-sets)):

```
{
    "resourceType": "Library",
    "id": "cvd-conditions",
    "url": "https://example.org/Library/cvd-conditions",
    "name": "CvdConditions",
    "status": "active",
    "type": {
        "coding": [
            {
                "system": "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
                "code": "sql-view"
            }
        ]
    },
    "relatedArtifact": [
        {
            "type": "depends-on",
            "resource": "https://example.org/ViewDefinition/conditions",
            "label": "conditions"
        },
        {
            "type": "depends-on",
            "resource": "http://example.org/ValueSet/cardiovascular-disease|2026",
            "label": "cvd_codes"
        }
    ],
    "content": [
        {
            "contentType": "application/sql",
            "data": "U0VMRUNUIGNvbmRpdGlvbnMucGF0aWVudF9pZCwgY29uZGl0aW9ucy5zeXN0ZW0sIGNvbmRpdGlvbnMuY29kZQpGUk9NIGNvbmRpdGlvbnMKV0hFUkUgRVhJU1RTICgKICBTRUxFQ1QgMSBGUk9NIGN2ZF9jb2RlcwogIFdIRVJFIGN2ZF9jb2Rlcy5zeXN0ZW0gPSBjb25kaXRpb25zLnN5c3RlbQogICAgQU5EIGN2ZF9jb2Rlcy5jb2RlID0gY29uZGl0aW9ucy5jb2RlCik="
        }
    ]
}
```

The `data` decodes to:

```
SELECT conditions.patient_id, conditions.system, conditions.code
FROM conditions
WHERE EXISTS (
  SELECT 1 FROM cvd_codes
  WHERE cvd_codes.system = conditions.system
    AND cvd_codes.code = conditions.code
)
```

This SQLQuery counts the patients in the SQLView whose code is drawn from a code system chosen at run time:

```
{
    "resourceType": "Library",
    "id": "cvd-patient-count",
    "url": "https://example.org/Library/cvd-patient-count",
    "name": "CvdPatientCount",
    "status": "active",
    "type": {
        "coding": [
            {
                "system": "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
                "code": "sql-query"
            }
        ]
    },
    "relatedArtifact": [
        {
            "type": "depends-on",
            "resource": "https://example.org/Library/cvd-conditions",
            "label": "cvd_conditions"
        }
    ],
    "parameter": [{ "name": "system", "use": "in", "type": "string" }],
    "content": [
        {
            "contentType": "application/sql",
            "data": "U0VMRUNUIENPVU5UKERJU1RJTkNUIHBhdGllbnRfaWQpIEFTIHBhdGllbnRzCkZST00gY3ZkX2NvbmRpdGlvbnMKV0hFUkUgc3lzdGVtID0gOnN5c3RlbQ=="
        }
    ]
}
```

```
SELECT COUNT(DISTINCT patient_id) AS patients
FROM cvd_conditions
WHERE system = :system
```

### The SQL[​](#the-sql "Direct link to The SQL")

The SQL is [Spark SQL](https://spark.apache.org/docs/latest/sql-ref.html), and it is validated before it runs:

* It must be read-only: a query, optionally with `WITH` clauses. DDL, DML and other commands are rejected, except `DESCRIBE <label>`, which lists the columns of a dependency, and `DESCRIBE QUERY <query>`.
* Every table it reads must be a declared dependency label or a common table expression defined within the query. Any other table name, or a direct file read, is rejected.
* Only built-in Spark functions may be called. Pathling's own functions, such as its terminology functions, belong in a ViewDefinition and are rejected in SQL: membership and translation in SQL are expressed with joins to a [value set](#value-sets) or [concept map](#concept-maps) instead.

A fault in the SQL that only Spark's analyser can catch - an unresolved column, an unknown function, a missing `GROUP BY`, an ambiguous reference - is a `422`, and its diagnostics carry the analyser's own message, including any suggested identifier. The analyser sees the query as rewritten for execution rather than as submitted: each table reference naming a dependency label is replaced with an internal request-scoped view name before analysis, so a reported line and column position can differ from the position in the submitted text, and a qualified suggestion can name one of those internal views. Only table references are replaced, so a column, alias or function that happens to share a label's name is left as submitted.

### Parameters[​](#parameters "Direct link to Parameters")

A SQLQuery declares each runtime parameter in `Library.parameter`, with a `name`, a `type` and `use` set to `in`, and refers to it in the SQL as `:name`. The request binds each one through the `parameters` resource (on `$sql-export`, the `parameters` part of the subject), with an entry whose name and `value[x]` type match the declaration:

```
{
    "name": "parameters",
    "resource": {
        "resourceType": "Parameters",
        "parameter": [
            { "name": "system", "valueString": "http://snomed.info/sct" }
        ]
    }
}
```

The supported types are `string`, `code`, `id`, `uri`, `url`, `canonical`, `oid`, `uuid`, `integer`, `unsignedInt`, `positiveInt`, `boolean`, `decimal`, `date`, `dateTime`, `instant`, `time` and `base64Binary`.

Every declared parameter must be bound. The SQL engine has no parameter defaults, so an unbound declaration cannot be executed at all, and leaving one out is a `400` rather than a query run over a missing value. Each unbound declaration is reported as its own `invalid` issue naming `parameters`, even when the request carried no `parameters` resource, so a request short of several bindings is answered naming all of them. A binding that names an undeclared parameter, repeats one, or supplies a value of the wrong type is also a `400`.

## Dependencies[​](#dependencies "Direct link to Dependencies")

A SQLQuery or SQLView declares every table its SQL reads as a `relatedArtifact` entry with `type` set to `depends-on`, a `resource` carrying the canonical URL of the artefact, and a `label`:

```
{
    "type": "depends-on",
    "resource": "https://example.org/ViewDefinition/conditions",
    "label": "conditions"
}
```

The SQL refers to the table by its label alone, never by its URL. A label must match `^[A-Za-z][A-Za-z0-9_]*$` and be unique within the Library. The `resource` must be an absolute canonical URL (`http://`, `https://` or `urn:`), optionally pinned with `|version`; a pinned reference matches only that version.

A dependency can be any of:

* a ViewDefinition;
* a SQLView, which can in turn depend on further ViewDefinitions and SQLViews;
* an [external table](#external-tables) configured by the operator;
* a [value set](#value-sets); or
* a [concept map](#concept-maps).

A SQLQuery cannot be a dependency; referencing one is a `400`. To reuse SQL, write it as a SQLView.

### Dependency graphs[​](#dependency-graphs "Direct link to Dependency graphs")

A SQLView dependency is resolved recursively, so a subject and its dependencies form a graph. In the examples above, the SQLQuery `cvd-patient-count` reads the SQLView `cvd-conditions`, which reads the ViewDefinition `conditions` and the value set `cardiovascular-disease`:

<!-- -->

Running the query needs only the query to be named; the server resolves the rest:

```
POST [base]/$sql-run
Content-Type: application/fhir+json

{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "subjectCanonical",
      "valueCanonical": "https://example.org/Library/cvd-patient-count"
    },
    {
      "name": "parameters",
      "resource": {
        "resourceType": "Parameters",
        "parameter": [{ "name": "system", "valueString": "http://snomed.info/sct" }]
      }
    }
  ]
}
```

The graph is resolved, and every fault in it reported, before any SQL runs:

* An artefact reached by more than one path (a diamond) is resolved once and shared, including a bare and a `|version` reference that resolve to the same stored artefact.
* A cycle is a `400`.
* A graph nested deeper than [`pathling.sqlQuery.maxDependencyDepth`](/docs/server/configuration.md#sql-query) (default `10`) is a `400`. The subject's direct dependencies sit at depth one.
* A reference that nothing resolves is a `404` naming the label and the reference.

### Resolution order[​](#resolution-order "Direct link to Resolution order")

Each canonical URL is resolved against these sources, in order:

1. a `context` entry supplied with the request (see [Supplying artefacts inline](#supplying-artefacts-inline));
2. a configured external table, a stored ViewDefinition or a stored SQLView. A URL that matches more than one of these is ambiguous and is a `400` naming the label;
3. the terminology layer, as a [value set](#value-sets); and
4. the terminology layer, as a [concept map](#concept-maps).

The terminology layer is consulted last so that a view dependency never costs a terminology request. A stored or configured artefact that happens to share a value set's or concept map's URL therefore wins silently; that is an operator-side collision, not a request fault.

### Supplying artefacts inline[​](#supplying-artefacts-inline "Direct link to Supplying artefacts inline")

The `context` parameter supplies a ViewDefinition, SQLView, ValueSet or ConceptMap with the request, for a dependency the server does not hold or to override one it does. On `$sql-export`, `context` applies to every subject in the job and produces no output of its own. This runs an inline query whose view is also inline, without storing either:

```
POST [base]/$sql-run
Content-Type: application/fhir+json

{
  "resourceType": "Parameters",
  "parameter": [
    { "name": "subjectResource", "resource": { "resourceType": "Library", "...": "..." } },
    { "name": "context", "resource": { "resourceType": "ViewDefinition", "...": "..." } }
  ]
}
```

A `context` entry is matched to a dependency by `url` and, where the dependency is pinned, by an equal `version`. It outranks anything the server could resolve for that URL. Every entry must carry a `url`, two entries must not share one, and an entry that matches no dependency of the subject is a `400`: it usually means a URL was mistyped, and ignoring it would run the request against different artefacts than the client intended.

### External tables[​](#external-tables "Direct link to External tables")

A dependency may name an external table configured by the operator under [`pathling.sqlQuery.externalTables`](/docs/server/configuration.md#sql-query), by the `url` it was configured under. This is a read-only Delta or Parquet table outside the warehouse, such as a reference data set, that the SQL can join to its FHIR views. The reference takes the same form as a ViewDefinition dependency, and `DESCRIBE <label>` lists its columns. An external table has no version, so a reference carrying a `|version` pin never matches one.

### Data filters[​](#data-filters "Direct link to Data filters")

The `patient`, `group` and `_since` parameters restrict the FHIR data that the ViewDefinitions in the graph read. They do not filter an external table, a value set or a concept map, whose rows are constrained only by the SQL that joins them.

## Terminology in SQL[​](#terminology-in-sql "Direct link to Terminology in SQL")

A query over FHIR data often asks a terminology question: *which patients have a diagnosis in the cardiovascular disease value set?* is a question about value set membership, and *report each diagnosis as its ICD-10 code* is a question about translation through a concept map. The SQL on FHIR [Terminology in SQL](https://build.fhir.org/ig/HL7/sql-on-fhir/en/terminology-in-sql.html) page specifies how a SQLQuery or SQLView answers these with SQL: it declares the ValueSet or ConceptMap as a dependency, and the server exposes its content as a table with fixed columns, joined to like any other.

Both `$sql-run` and `$sql-export` support this, for a SQLQuery or SQLView subject and for SQLViews anywhere in its dependency graph. A ViewDefinition subject uses the FHIRPath terminology functions instead.

### Declaring a terminology dependency[​](#declaring-a-terminology-dependency "Direct link to Declaring a terminology dependency")

A value set or concept map is declared in the same way as any other dependency, and the SQL refers to it by its label. The SQL never names a canonical URL or a terminology server:

```
"relatedArtifact": [
    {
        "type": "depends-on",
        "resource": "https://example.org/ViewDefinition/conditions",
        "label": "conditions"
    },
    {
        "type": "depends-on",
        "resource": "http://example.org/ValueSet/cardiovascular-disease|2026",
        "label": "cvd_codes"
    },
    {
        "type": "depends-on",
        "resource": "http://example.org/ConceptMap/sct-to-icd10|2026",
        "label": "sct_to_icd10"
    }
]
```

Pin the version where you can. The membership of an unpinned value set, or the mappings of an unpinned concept map, change each time the artefact is republished, so two runs of the same query can return different rows for reasons the query does not record.

Implicit value sets and concept maps work as dependencies too, subject to the terminology mode (see [Where terminology comes from](#where-terminology-comes-from)). For example:

| Canonical URL                                                                  | Kind                                                           |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------- |
| `http://snomed.info/sct?fhir_vs=isa/49601007`                                  | Value set: SNOMED CT subsumption                               |
| `http://fhir.org/VCL?v1=%28http%3A%2F%2Fsnomed.info%2Fsct%29inactive+%3D+true` | Value set: a ValueSet Compose Language expression, URL encoded |
| `http://snomed.info/sct?fhir_cm=900000000000527005`                            | Concept map: SNOMED CT SAME AS associations                    |

### Value sets[​](#value-sets "Direct link to Value sets")

A value set dependency is exposed under its label as a table with exactly these columns:

| Column     | Type      | Nullable | Content                                                                                 |
| ---------- | --------- | -------- | --------------------------------------------------------------------------------------- |
| `system`   | `string`  | no       | The code system canonical URL.                                                          |
| `version`  | `string`  | yes      | The code system version recorded against the member, where the source records one.      |
| `code`     | `string`  | no       | The member code.                                                                        |
| `display`  | `string`  | yes      | The display text, where the source records one.                                         |
| `inactive` | `boolean` | yes      | `true` for an inactive member, `false` only where an expansion says so, otherwise null. |

Rows are unique on (`system`, `version`, `code`). Where the membership comes from a FHIR expansion, every non-abstract `contains` entry at any depth of nesting contributes a row, and abstract entries contribute none. `version` is taken from the entry's own `version` only. A terminology server that records code system versions once for the whole expansion, as Ontoserver does in its `version` and `used-codesystem` parameters, leaves the column null; the version the membership was computed against then appears only in the [provenance](#resolution-and-provenance) line logged for the request. A pinned reference is expanded at that version and no other; an unpinned one leaves the choice of version to the terminology layer.

The usual idioms are a semi-join, which keeps each FHIR row once however many members it matches:

```
SELECT conditions.patient_id, conditions.code
FROM conditions
WHERE EXISTS (
  SELECT 1 FROM cvd_codes
  WHERE cvd_codes.system = conditions.system
    AND cvd_codes.code = conditions.code
)
```

its negation, `NOT EXISTS`, which excludes the members of a value set, and an inner join, which brings the member's `display` and `inactive` columns alongside the FHIR row:

```
SELECT conditions.patient_id, conditions.code, cvd_codes.display
FROM conditions
JOIN cvd_codes
  ON cvd_codes.system = conditions.system
  AND cvd_codes.code = conditions.code
```

Prefer the semi-join where only membership matters. The table can hold the same code under two versions of one code system, and a join on `system` and `code` alone would then return the FHIR row twice.

Whether inactive codes appear in the table is decided by the terminology layer, not by the server, so a query that wants active members only should say so explicitly, remembering that the column is null rather than `false` for most active members: `WHERE cvd_codes.inactive IS NULL OR cvd_codes.inactive = FALSE`.

A membership larger than [`pathling.sqlQuery.valueSetMaxMembers`](/docs/server/configuration.md#sql-query) (default `100000`) is a `422` naming the label, the URL and the maximum.

### Concept maps[​](#concept-maps "Direct link to Concept maps")

A concept map dependency is exposed under its label as a table with exactly these columns:

| Column           | Type     | Nullable | Content                                                                                   |
| ---------------- | -------- | -------- | ----------------------------------------------------------------------------------------- |
| `source_system`  | `string` | no       | The code system canonical URL of the source code.                                         |
| `source_version` | `string` | yes      | The code system version the mapping was authored against, where the map records one.      |
| `source_code`    | `string` | no       | The source code.                                                                          |
| `source_display` | `string` | yes      | The display text for the source code, where the map records one.                          |
| `target_system`  | `string` | yes      | The code system canonical URL the mapping translates into, where the map names one.       |
| `target_version` | `string` | yes      | The version of the target code system, where the map records one.                         |
| `target_code`    | `string` | yes      | The target code; null where the map states that the source code has no mapping.           |
| `target_display` | `string` | yes      | The display text for the target code, where the map records one.                          |
| `relationship`   | `string` | yes      | How the source relates to the target; null where the map states that there is no mapping. |

Each `group.element.target` whose `equivalence` is not `unmatched` contributes one row, populated from the group's `source` and `target`, the element's `code` and `display` and the target's `code` and `display`. An element with at least one `unmatched` target also contributes one *no-mapping* row, which carries the group's target system and version but a null `target_code`, `target_display` and `relationship`; a code carried by an `unmatched` target is not exposed. `source_version` and `target_version` come from `group.sourceVersion` and `group.targetVersion`; where the version element is absent and the URI in `group.source` or `group.target` carries a `|version` suffix, the suffix is split off into the version column, so that both columns compare directly with a `Coding.system` and `Coding.version` projected from FHIR data. A group without a `target` is accepted and its rows have a null `target_system` and `target_version`. An element with no targets contributes no rows, and a map with no mappings yields a table with no rows rather than an error.

Rows are unique on every column except the two displays, two nulls counting as equal; where a map lists an element twice in a group, or gives an element two targets with the same code and equivalence, the table holds one row carrying the displays of the first occurrence.

Pathling holds R4 resources, whose targets carry an `equivalence` rather than the `relationship` the SQL on FHIR specification defines, so `relationship` is converted as the official R4 to R5 conversion of ConceptMap does:

| R4 `equivalence`          | `relationship`                   |
| ------------------------- | -------------------------------- |
| `equal`, `equivalent`     | `equivalent`                     |
| `wider`, `subsumes`       | `source-is-narrower-than-target` |
| `narrower`, `specializes` | `source-is-broader-than-target`  |
| `relatedto`, `inexact`    | `related-to`                     |
| `disjoint`                | `not-related-to`                 |
| `unmatched`               | a no-mapping row                 |

Translation needs the target columns, so it is a join rather than a semi-join, and a `LEFT JOIN` keeps the FHIR rows the map does not translate:

```
SELECT conditions.patient_id,
       conditions.code,
       sct_to_icd10.target_code,
       sct_to_icd10.relationship
FROM conditions
LEFT JOIN sct_to_icd10
  ON sct_to_icd10.source_system = conditions.system
 AND sct_to_icd10.source_code = conditions.code
 AND (sct_to_icd10.relationship IS NULL
      OR sct_to_icd10.relationship <> 'not-related-to')
```

A `relationship` of `not-related-to` asserts that the target is *not* a translation of the source, yet the row carries a target code, so a query that translates should exclude it. The exclusion belongs in the `ON` clause, since in `WHERE` it would also drop the rows the `LEFT JOIN` was written to keep, and it must admit null, since a no-mapping row has no `relationship`.

After the join, each FHIR row falls into one of three cases, told apart by which columns are null:

* a translated row has a `target_code`;
* a row the map states has no mapping has `source_code` populated and `target_code` null; and
* a row the map is silent about, because its code or its whole code system is absent from the map, has every column of the table null.

A FHIR row can match several mappings, where the map gives its code several targets or targets in several code systems, and is then returned once per mapping; a `target_system` predicate selects one target code system, and `relationship = 'equivalent'` keeps only exact translations. Because the table has two sides, the same map also translates in the reverse direction: a join on `target_system` and `target_code` that selects `source_code` needs no second artefact, although several source codes commonly map to one target, so it multiplies rows more readily:

```
SELECT conditions.patient_id, sct_to_icd10.source_code
FROM conditions
JOIN sct_to_icd10
  ON sct_to_icd10.target_system = conditions.system
 AND sct_to_icd10.target_code = conditions.code
```

A translated code can be tested against a value set by semi-joining the target columns to a value set table:

```
SELECT conditions.patient_id, sct_to_icd10.target_code
FROM conditions
JOIN sct_to_icd10
  ON sct_to_icd10.source_system = conditions.system
 AND sct_to_icd10.source_code = conditions.code
 AND sct_to_icd10.relationship = 'equivalent'
WHERE EXISTS (
  SELECT 1 FROM cvd_codes
  WHERE cvd_codes.system = sct_to_icd10.target_system
    AND cvd_codes.code = sct_to_icd10.target_code
)
```

`group.unmapped` is not applied: the table carries the map's explicit mappings only, unlike the `$translate` operation, which applies the default. A query that wants unmapped codes passed through writes it itself, as `COALESCE(sct_to_icd10.target_code, conditions.code)`.

A map whose content no flat row can represent is rejected with a `422` before any SQL runs, rather than exposed incompletely: a group without a `source`, a target carrying `dependsOn` or `product`, an element without a `code`, a target without a `code` whose equivalence is not `unmatched`, and a target without an `equivalence`. The issue names the label, the canonical URL, the fault and, where there is one, the source code of the mapping at fault. A map with more rows than [`pathling.sqlQuery.conceptMapMaxMappings`](/docs/server/configuration.md#sql-query) (default `100000`) is a `422` naming the label, the URL and the maximum.

An unpinned reference to a map held in several versions resolves to the latest, compared as the local terminology store compares code system versions (eight-digit dates as dates, SemVer versions by SemVer precedence, anything else segment by segment). Where no latest can be determined, as for versions `1.0.0+build1` and `1.0.0+build2`, whose build metadata takes no part in SemVer precedence, the request is a `404` stating that the version to use cannot be determined.

### Where terminology comes from[​](#where-terminology-comes-from "Direct link to Where terminology comes from")

A value set or concept map that is not supplied inline is obtained from the terminology layer configured under [`pathling.terminology`](/docs/server/configuration.md#terminology-service):

| Mode               | Value sets                                                                                        | Concept maps                                                                                                                                                        |
| ------------------ | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server` (default) | `ValueSet/$expand` on `pathling.terminology.serverUrl`.                                           | A ConceptMap search and read on the terminology server. SNOMED CT `fhir_cm` implicit maps are not synthesised, so only a map the server holds at that URL is found. |
| `local`            | Computed by the [local terminology store](/docs/server/configuration.md#local-terminology-store). | ConceptMaps imported into the store, and the SNOMED CT implicit maps for the SAME AS, REPLACED BY, POSSIBLY EQUIVALENT TO and ALTERNATIVE reference sets.           |
| disabled           | Only a `context` ValueSet with an `expansion`.                                                    | Only a `context` ConceptMap.                                                                                                                                        |

The details of each mode, including which URLs skip the value set or concept map lookup and how terminology server failures are reported, are described under [SQL query configuration](/docs/server/configuration.md#sql-query).

### Supplying a value set or concept map inline[​](#supplying-a-value-set-or-concept-map-inline "Direct link to Supplying a value set or concept map inline")

A `context` entry can be a ValueSet or ConceptMap, matched like any other [inline artefact](#supplying-artefacts-inline). This is useful for a value set or map the terminology layer does not hold, or to fix the exact membership a query runs against:

```
{
    "name": "context",
    "resource": {
        "resourceType": "ValueSet",
        "url": "http://example.org/ValueSet/cardiovascular-disease",
        "version": "2026",
        "status": "active",
        "expansion": {
            "timestamp": "2026-09-16T00:00:00Z",
            "contains": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "22298006",
                    "display": "Myocardial infarction"
                },
                {
                    "system": "http://hl7.org/fhir/sid/icd-10",
                    "code": "I21",
                    "display": "Acute myocardial infarction"
                }
            ]
        }
    }
}
```

A supplied ValueSet carrying an `expansion` supplies the membership as-is, and the terminology layer is not consulted for it. That expansion must be complete, so one carrying an `offset`, or whose `total` exceeds its number of entries, is a `422`, as is one with an entry that does not identify a member (a non-abstract entry without `code`, or one with `code` but no `system`). A supplied ValueSet carrying only a `compose` is expanded by the terminology layer. One carrying neither defines no membership and is a `422`.

A supplied ConceptMap is converted to rows as-is, without consulting the terminology layer, so it works even with terminology disabled. It is subject to the same rules and rejections as a map from the terminology layer, and a fault in it names `context` in the issue's `expression`.

### Resolution and provenance[​](#resolution-and-provenance "Direct link to Resolution and provenance")

Every value set and concept map in a request is resolved before any SQL runs, and a fault in any of them rejects the request rather than running the SQL over partial content. Each canonical, as written, is resolved once, and every reference to it - under several labels, or through several SQLViews - sees the same content. A pinned and an unpinned reference to the same URL are two canonicals and two tables.

* On `$sql-run`, the content is current at the time of the request; a change on the terminology server between two requests is seen by the second.
* On `$sql-export`, the content is obtained at kick-off, shared by every subject in the job and fixed for the life of the job. Every terminology fault is reported at kick-off, before a job is created. The content sits outside the job's data snapshot, so two kick-offs that differ only in the membership or mappings of a `context` ValueSet or ConceptMap are two jobs.

Every resolution is logged at `INFO`, so a result can be traced to the content it was computed against. For a value set, the line records the canonical URL, the version resolved, the source, the member count, the expansion identifier and timestamp where the source records them, and the code system versions. For a concept map, it records the canonical URL, the version resolved (or `none`), the source - `context`, the terminology server's URL or `local store` - and the row count:

```
Resolved concept map 'http://example.org/ConceptMap/sct-to-icd10' (version 2026) from https://tx.example.org/fhir: 3 mappings
```

A value set or concept map dependency needs no authority beyond the operation's own; see [authorization](/docs/server/authorization.md).

### Example[​](#example "Direct link to Example")

Suppose the server holds four Conditions, and the `conditions` view produces:

| patient\_id  | system                           | code        |
| ------------ | -------------------------------- | ----------- |
| `Patient/p1` | `http://snomed.info/sct`         | `22298006`  |
| `Patient/p2` | `http://hl7.org/fhir/sid/icd-10` | `I21`       |
| `Patient/p3` | `http://snomed.info/sct`         | `73211009`  |
| `Patient/p4` | `http://snomed.info/sct`         | `102499006` |

This export runs two subjects in one job: the stored [`cvd-patient-count`](#sqlqueries-and-sqlviews) query, and an inline query that translates each condition to ICD-10 with the `LEFT JOIN` shown [above](#concept-maps). The value set and the concept map are both supplied as `context`, so the job needs no terminology server:

```
POST [base]/$sql-export
Content-Type: application/fhir+json
Prefer: respond-async

{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "subject",
      "part": [
        { "name": "name", "valueString": "cvd_patients" },
        {
          "name": "subjectCanonical",
          "valueCanonical": "https://example.org/Library/cvd-patient-count"
        },
        {
          "name": "parameters",
          "resource": {
            "resourceType": "Parameters",
            "parameter": [{ "name": "system", "valueString": "http://snomed.info/sct" }]
          }
        }
      ]
    },
    {
      "name": "subject",
      "part": [
        { "name": "name", "valueString": "icd10" },
        {
          "name": "subjectResource",
          "resource": {
            "resourceType": "Library",
            "url": "https://example.org/Library/conditions-to-icd10",
            "name": "ConditionsToIcd10",
            "status": "active",
            "type": {
              "coding": [
                {
                  "system": "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
                  "code": "sql-query"
                }
              ]
            },
            "relatedArtifact": [
              {
                "type": "depends-on",
                "resource": "https://example.org/ViewDefinition/conditions",
                "label": "conditions"
              },
              {
                "type": "depends-on",
                "resource": "http://example.org/ConceptMap/sct-to-icd10|2026",
                "label": "sct_to_icd10"
              }
            ],
            "content": [{ "contentType": "application/sql", "data": "..." }]
          }
        }
      ]
    },
    {
      "name": "context",
      "resource": {
        "resourceType": "ValueSet",
        "url": "http://example.org/ValueSet/cardiovascular-disease",
        "version": "2026",
        "...": "..."
      }
    },
    {
      "name": "context",
      "resource": {
        "resourceType": "ConceptMap",
        "url": "http://example.org/ConceptMap/sct-to-icd10",
        "version": "2026",
        "status": "active",
        "group": [
          {
            "source": "http://snomed.info/sct",
            "target": "http://hl7.org/fhir/sid/icd-10|2019",
            "element": [
              {
                "code": "22298006",
                "target": [{ "code": "I21", "equivalence": "equivalent" }]
              },
              {
                "code": "73211009",
                "target": [{ "code": "E14", "equivalence": "narrower" }]
              },
              {
                "code": "102499006",
                "target": [{ "equivalence": "unmatched" }]
              }
            ]
          }
        ]
      }
    },
    { "name": "_format", "valueCode": "csv" }
  ]
}
```

The `cvd_patients` output counts one patient: `Patient/p1` is the only one with a SNOMED CT code in the value set, since `p2`'s member code is from ICD-10.

```
patients
1
```

The `icd10` output has one row per condition:

```
patient_id,code,target_code,relationship
Patient/p1,22298006,I21,equivalent
Patient/p2,I21,,
Patient/p3,73211009,E14,source-is-broader-than-target
Patient/p4,102499006,,
```

`p1` and `p3` are translated. `p2` and `p4` both lack a `target_code`, for different reasons: `p2` is already an ICD-10 code, so the map is silent about it, whereas the map states that `p4`'s code has no mapping. A query that needs to tell the two apart selects `sct_to_icd10.source_code`, which is null only for `p2`.

The same `context` entries work unchanged on `$sql-run`, which runs one of the subjects and returns its rows in the response body.
