---
sidebar_position: 7
description: The sql-run operation synchronously executes a ViewDefinition, SQLQuery or SQLView and returns the result.
---

# Run

The `$sql-run` operation executes a single SQL on FHIR _subject_ and returns
its result in the response body. A subject is one of three artefacts:

- a
  [ViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html),
  which projects a resource type into a flat table;
- a
  [SQLQuery](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLQuery.html)
  Library, which runs SQL over the tables its `relatedArtifact` entries name; or
- a
  [SQLView](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLView.html)
  Library, which is a named, reusable SQL table source.

The operation is subject-polymorphic: which kind you supply decides which
parameters apply and which output formats are available, but the endpoint,
filters and error contract are the same for all three.

Use [`$sql-export`](sql-export.md) instead when the result is too large or too
slow to return synchronously, or when you want to export several subjects
together.

## Endpoint

```
GET  [base]/$sql-run
POST [base]/$sql-run
```

The operation is system level only. A `GET` can carry only primitive
parameters, so it is available when the subject is already stored on the
server; supplying a resource-valued parameter over `GET` is rejected with a
`400`.

## Parameters

| Name               | Cardinality | Type       | Description                                                                                                                             |
| ------------------ | ----------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `subjectCanonical` | 0..1        | canonical  | The subject's canonical URL, honouring a `\|version` pin. Matched against both `ViewDefinition.url` and `Library.url`.                  |
| `subjectReference` | 0..1        | Reference  | A relative reference naming its type: `ViewDefinition/[id]` or `Library/[id]`.                                                          |
| `subjectResource`  | 0..1        | Resource   | An inline ViewDefinition, SQLQuery or SQLView.                                                                                          |
| `parameters`       | 0..1        | Parameters | Runtime bindings for a SQL subject. Every `Library.parameter` declaration must be bound by an entry matching it in name and type.       |
| `context`          | 0..\*       | Resource   | Inline supporting artefacts (ViewDefinition, SQLView or ValueSet) for dependencies the server cannot resolve. Matched by canonical URL. |
| `resource`         | 0..\*       | string     | FHIR resources to project instead of server data, for a ViewDefinition subject. Each is a serialised resource or a Bundle to unwrap.    |
| `_format`          | 0..1        | code       | Output format; see below. Takes precedence over the `Accept` header.                                                                    |
| `header`           | 0..1        | boolean    | Include the header row in CSV output. Defaults to `true`.                                                                               |
| `_limit`           | 0..1        | integer    | Maximum rows to return. When omitted, the whole result is returned.                                                                     |
| `patient`          | 0..\*       | Reference  | Restricts the data the subject reads to these patients' compartments.                                                                   |
| `group`            | 0..\*       | Reference  | Restricts the data the subject reads to these groups' member patients.                                                                  |
| `_since`           | 0..1        | instant    | Restricts to resources updated at or after this instant.                                                                                |
| `source`           | 0..1        | string     | **Not supported**: an external data source. Supplying it is rejected with a `400`.                                                      |

Exactly one of `subjectCanonical`, `subjectReference` and `subjectResource`
must be supplied; naming none, or more than one, is a `400`.

`parameters` applies only to a SQL subject, and `resource` only to a
ViewDefinition subject. Supplying either to the other kind is a `400` naming
the parameter, rather than being silently ignored.

## Output formats

The formats on offer depend on the kind of subject:

| Format    | ViewDefinition | SQLQuery / SQLView | Media type                       |
| --------- | -------------- | ------------------ | -------------------------------- |
| `ndjson`  | yes (default)  | yes (default)      | `application/x-ndjson`           |
| `csv`     | yes            | yes                | `text/csv`                       |
| `json`    | yes            | yes                | `application/json`               |
| `parquet` | no             | yes                | `application/vnd.apache.parquet` |
| `fhir`    | no             | yes                | `application/fhir+json`          |

An explicit `_format` is parsed strictly: an unrecognised value, or one not
available for the resolved kind, is a `400` naming `_format`. With no
`_format`, the format is negotiated from the `Accept` header, falling back to
NDJSON when nothing matches; content negotiation never fails, since a client
that sends a header the server cannot honour still wants a result.

## Supporting artefacts

A SQL subject reaches its table sources through `relatedArtifact` references
resolved by canonical URL. A `context` entry offers an artefact the server does
not hold, and outranks server resolution when both are available. Every entry
must carry a `url`, since that is what a dependency is matched by, and an entry
that matches no dependency of the subject is a `400`: it usually means a URL
was mistyped, and ignoring it would run the request against different artefacts
than the client intended.

A `relatedArtifact` may also name an
[external table](../configuration#sql-query) configured by the operator, by
the `url` it was configured under. The reference takes the same form as a
ViewDefinition dependency, and the SQL reaches the table only through the
declared `label`, which obeys the usual rule (`^[A-Za-z][A-Za-z0-9_]*$`,
unique within the Library). An external table has no version, so a reference
carrying a `|version` pin never matches one. A URL that matches both a
configured table and a stored ViewDefinition or SQLView is ambiguous and is a
`400` naming the label; a `context` entry with that URL outranks the table, as
it outranks server resolution generally. `DESCRIBE <label>` lists the table's
columns just as it does for any other dependency.

The `patient`, `group` and `_since` filters restrict the FHIR data the subject
reads; they do not filter an external table or a value set, whose rows are
constrained only by the SQL that joins them.

### Value sets

A `relatedArtifact` may also name a value set, so that the SQL can test code
membership with an ordinary join rather than enumerating codes by hand or
routing the question through a ViewDefinition. The declaration takes the same
form as a view dependency: an absolute canonical URL, optionally pinned with
`|version`, and a `label`:

```json
{
    "type": "depends-on",
    "resource": "http://example.org/ValueSet/cardiovascular-disease|2026",
    "label": "cvd_codes"
}
```

A canonical URL is treated as a value set only as a last resort: a `context`
entry, a configured external table, a stored ViewDefinition and a stored SQLView
are all tried first, in that order of precedence, and the terminology layer is
asked only when none of them matches. A URL that the terminology layer cannot
resolve either is a `404` naming the label and the reference. A stored or
configured artefact that happens to share a value set's URL therefore wins
silently; that is an operator-side collision, not a request fault.

Membership comes from the terminology layer the server is configured with -
`$expand` on a FHIR terminology server, or the local terminology store - and is
exposed under the label as a relation with exactly these columns:

| Column     | Type      | Nullable | Content                                                                                 |
| ---------- | --------- | -------- | --------------------------------------------------------------------------------------- |
| `system`   | `string`  | no       | The code system canonical URL.                                                          |
| `version`  | `string`  | yes      | The code system version recorded against the member, where the source records one.      |
| `code`     | `string`  | no       | The member code.                                                                        |
| `display`  | `string`  | yes      | The display text, where the source records one.                                         |
| `inactive` | `boolean` | yes      | `true` for an inactive member, `false` only where an expansion says so, otherwise null. |

Rows are unique on (`system`, `version`, `code`). Where the membership comes
from a FHIR expansion, every non-abstract `contains` entry at any depth of
nesting contributes a row, and abstract entries contribute none. `version` is
taken from the entry's own `version` only. A terminology server that records
code system versions once for the whole expansion, as Ontoserver does in its
`version` and `used-codesystem` parameters, leaves the column null; the version
the membership was computed against then appears only in the provenance line
logged for the request. A pinned reference is expanded at that version and no
other; an unpinned one leaves the choice of version to the terminology layer.
`DESCRIBE <label>` lists the five columns as it does for any other dependency.

The value set is reached only through its label, so the SQL never names a
canonical URL or a terminology server. The usual idioms are a semi-join, which
keeps each FHIR row once however many members it matches:

```sql
SELECT conditions.patient_id, conditions.code
FROM conditions
WHERE EXISTS (
  SELECT 1 FROM cvd_codes
  WHERE cvd_codes.system = conditions.system
    AND cvd_codes.code = conditions.code
)
```

and an inner join, which brings the member's `display` and `inactive` columns
alongside the FHIR row:

```sql
SELECT conditions.patient_id, conditions.code, cvd_codes.display
FROM conditions
JOIN cvd_codes
  ON cvd_codes.system = conditions.system
  AND cvd_codes.code = conditions.code
```

Whether inactive codes appear in the relation is decided by the terminology
layer, not by the server, so a query that wants active members only should say
so explicitly, remembering that the column is null rather than `false` for most
active members: `WHERE cvd_codes.inactive IS NULL OR cvd_codes.inactive = FALSE`.

A `context` entry may be a ValueSet, matched to a dependency by `url` and, where
the dependency is pinned, by an equal `version`, under the same rules as a
supplied ViewDefinition or SQLView. It must carry a `url`, and it outranks
anything the server could resolve for that URL. A supplied ValueSet carrying an
`expansion` supplies the membership as-is and the terminology layer is not
consulted for it; that expansion must be complete, so one carrying an `offset`,
or whose `total` exceeds its number of entries, is a `422`, as is one with an
entry that does not identify a member (a non-abstract entry without `code`, or
one with `code` but no `system`). A supplied ValueSet carrying only a `compose`
is expanded by the terminology layer. One carrying neither defines no membership
and is a `422`.

Each value set canonical, as written, is resolved and expanded once per
request, before any SQL executes, and every reference to it - under several
labels, or through several SQLViews - sees the same membership; a pinned and an
unpinned reference to the same URL are two canonicals and two relations. The
membership is current at the time of the request; a change on the terminology
server between two requests is seen by the second. A membership larger than
[`pathling.sqlQuery.valueSetMaxMembers`](../configuration#sql-query) is a `422`
naming the label, the URL and the maximum. Every resolution is logged at `INFO`
with the canonical URL, the version resolved, the source and the member count,
so a result can be traced to the membership it was computed against.

## Examples

Run a stored view over `GET`, as CSV:

```
GET [base]/$sql-run?subjectCanonical=https://example.org/ViewDefinition/demographics&_format=csv
Accept: text/csv
```

Run a stored query with a bound parameter:

```
POST [base]/$sql-run
Content-Type: application/fhir+json
Accept: application/x-ndjson

{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "subjectReference",
      "valueReference": { "reference": "Library/patients-by-family" }
    },
    {
      "name": "parameters",
      "resource": {
        "resourceType": "Parameters",
        "parameter": [{ "name": "family", "valueString": "Smith" }]
      }
    }
  ]
}
```

Run entirely ad hoc, with an inline query and the view it depends on supplied
as `context`:

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

Run a view over data supplied with the request, rather than server data:

```
POST [base]/$sql-run
Content-Type: application/fhir+json

{
  "resourceType": "Parameters",
  "parameter": [
    { "name": "subjectResource", "resource": { "resourceType": "ViewDefinition", "...": "..." } },
    { "name": "resource", "valueString": "{\"resourceType\":\"Patient\",\"id\":\"p1\"}" }
  ]
}
```

Filter the data every subject reads:

```
GET [base]/$sql-run?subjectReference=ViewDefinition/demographics&patient=Patient/p1&patient=Patient/p2
```

## Status codes

| Status                      | Condition                                                                                                                                                                                                                                                                                                |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `200 OK`                    | Successful execution, in the negotiated format.                                                                                                                                                                                                                                                          |
| `400 Bad Request`           | No subject or more than one; an inapplicable conditional parameter; an unsupported `_format`; `source`; an unresolvable filter value.                                                                                                                                                                    |
| `404 Not Found`             | The subject's canonical or reference resolves to nothing, or a dependency matches no ViewDefinition, SQLView, external table or value set.                                                                                                                                                               |
| `422 Unprocessable Entity`  | The subject is of no admitted kind, or is conformant but cannot be processed (for example a column type `_format=fhir` cannot express).                                                                                                                                                                  |
| `422 Unprocessable Entity`  | A value set resolves but its membership cannot be determined: the terminology server returns an error or cannot be reached, the local store cannot evaluate the compose, or a supplied ValueSet has neither an `expansion` nor a `compose`. The issue names the label, the canonical URL and the reason. |
| `422 Unprocessable Entity`  | A value set's membership exceeds `pathling.sqlQuery.valueSetMaxMembers`; the issue names the label, the canonical URL and the maximum.                                                                                                                                                                   |
| `422 Unprocessable Entity`  | A supplied ValueSet's `expansion` is incomplete (carries an `offset`, or a `total` greater than its entry count) or has an entry that does not identify a member; the issue names `context`.                                                                                                             |
| `500 Internal Server Error` | An unexpected execution or infrastructure fault.                                                                                                                                                                                                                                                         |
| `500 Internal Server Error` | A configured external table cannot be read; the issue names the table's `url` and never its storage path.                                                                                                                                                                                                |

Every 4xx and 5xx response carries an
[OperationOutcome](https://hl7.org/fhir/R4/operationoutcome.html) whose issues
name the parameter at fault in `expression`. A request that is wrong in more
than one way is answered with one outcome naming every problem, so it can be
corrected in a single round trip.

Every parameter the subject declares must be bound. The SQL engine has no
parameter defaults, so an unbound declaration cannot be executed at all, and
leaving one out is a `400` rather than a query run over a missing value. Each
unbound declaration is reported as its own `invalid` issue, so a request short
of several bindings is answered naming all of them, and `expression` names
`parameters` even when the request carried no `parameters` resource - its
absence is the fault.

A fault in the subject's own SQL that only Spark's analyser can catch - an
unresolved column, an unknown function, a missing `GROUP BY`, an ambiguous
reference - is a `422`, and its diagnostics carry the analyser's own message,
including any suggested identifier. The analyser sees the query as rewritten
for execution rather than as submitted: each table reference naming a
dependency label is replaced with an internal request-scoped view name before
analysis, so a reported line and column position can differ from the position
in the submitted text, and a qualified suggestion can name one of those
internal views. Only table references are replaced, so a column, alias or
function that happens to share a label's name is left as submitted.

## Conformance

The operation declares the spec canonical
`http://hl7.org/fhir/uv/sql-on-fhir/OperationDefinition/SQLRun` in the server
[CapabilityStatement](https://hl7.org/fhir/R4/capabilitystatement.html), whose
`documentation` states the per-kind format sets and the parameters this server
declines. Pathling serves no OperationDefinition of its own for it.

## Configuration and authorisation

The operation is enabled by `pathling.operations.sqlRunEnabled` (default
`true`) and guarded by the `pathling:sql-run` authority. Running a subject also
requires `read` authority for the resource type it projects, and reading a
stored subject requires `read` authority for `ViewDefinition` or `Library`. A
value set dependency requires only the operation authority. See
[authorization](../authorization.md).

## Python client

```python
from pathling import PathlingContext

# Subjects and results are exchanged as FHIR Parameters and flat files, so any
# HTTP client will do; the example below uses requests directly.
import requests

response = requests.get(
    "https://example.org/fhir/$sql-run",
    params={
        "subjectReference": "ViewDefinition/demographics",
        "_format": "csv",
    },
    headers={"Accept": "text/csv"},
)
response.raise_for_status()
print(response.text)
```
