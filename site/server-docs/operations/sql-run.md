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

How to write each kind of subject, declare its dependencies and use value sets
and concept maps in its SQL is described on the
[SQL on FHIR](../sql-on-fhir.md) page, since it is common to this operation
and [`$sql-export`](sql-export.md). Use `$sql-export` instead when the result
is too large or too slow to return synchronously, or when you want to export
several subjects together.

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

| Name               | Cardinality | Type       | Description                                                                                                                                         |
| ------------------ | ----------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `subjectCanonical` | 0..1        | canonical  | The subject's canonical URL, honouring a `\|version` pin. Matched against both `ViewDefinition.url` and `Library.url`.                              |
| `subjectReference` | 0..1        | Reference  | A relative reference naming its type: `ViewDefinition/[id]` or `Library/[id]`.                                                                      |
| `subjectResource`  | 0..1        | Resource   | An inline ViewDefinition, SQLQuery or SQLView.                                                                                                      |
| `parameters`       | 0..1        | Parameters | Runtime bindings for a SQL subject. Every `Library.parameter` declaration must be bound by an entry matching it in name and type.                   |
| `context`          | 0..\*       | Resource   | Inline supporting artefacts (ViewDefinition, SQLView, ValueSet or ConceptMap) for dependencies the server cannot resolve. Matched by canonical URL. |
| `resource`         | 0..\*       | string     | FHIR resources to project instead of server data, for a ViewDefinition subject. Each is a serialised resource or a Bundle to unwrap.                |
| `_format`          | 0..1        | code       | Output format; see below. Takes precedence over the `Accept` header.                                                                                |
| `header`           | 0..1        | boolean    | Include the header row in CSV output. Defaults to `true`.                                                                                           |
| `_limit`           | 0..1        | integer    | Maximum rows to return. When omitted, the whole result is returned.                                                                                 |
| `patient`          | 0..\*       | Reference  | Restricts the data the subject reads to these patients' compartments.                                                                               |
| `group`            | 0..\*       | Reference  | Restricts the data the subject reads to these groups' member patients.                                                                              |
| `_since`           | 0..1        | instant    | Restricts to resources updated at or after this instant.                                                                                            |
| `source`           | 0..1        | string     | **Not supported**: an external data source. Supplying it is rejected with a `400`.                                                                  |

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

In the `ndjson`, `json` and `csv` formats, a `DATE` value is written as an ISO
8601 date (`2026-01-02`) and a `TIMESTAMP` value as an ISO 8601 instant in UTC
(`2026-01-02T03:04:05Z`), whatever the locale and time zone of the server. These
are the same strings that the `fhir` format carries in `valueDate` and
`valueInstant`. A timestamp parsed from text without a zone, such as the literal
`TIMESTAMP'2026-01-02 03:04:05'`, is still read in the Spark session time zone
before it is written as an instant.

An explicit `_format` is parsed strictly: an unrecognised value, or one not
available for the resolved kind, is a `400` naming `_format`. With no
`_format`, the format is negotiated from the `Accept` header, falling back to
NDJSON when nothing matches; content negotiation never fails, since a client
that sends a header the server cannot honour still wants a result.

## Supporting artefacts

The `context` parameter supplies ViewDefinitions, SQLViews, ValueSets and
ConceptMaps that the subject depends on, inline with the request. How a
subject's dependencies are resolved, and how `context` entries are matched to
them, is described under [Dependencies](../sql-on-fhir.md#dependencies). Value
set and concept map dependencies are described under
[Terminology in SQL](../sql-on-fhir.md#terminology-in-sql). On `$sql-run`, each
value set and concept map is resolved once per request, and its content is
current at the time of the request.

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

| Status                      | Condition                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `200 OK`                    | Successful execution, in the negotiated format.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `400 Bad Request`           | No subject or more than one; an inapplicable conditional parameter; an unsupported `_format`; `source`; an unresolvable filter value; a `context` entry, of any kind, without a `url`, sharing its `url` with another entry, or matching no dependency.                                                                                                                                                                                                                                                                                                        |
| `404 Not Found`             | The subject's canonical or reference resolves to nothing, or a dependency matches no ViewDefinition, SQLView, external table, concept map or value set. The issue names the label and the reference.                                                                                                                                                                                                                                                                                                                                                           |
| `404 Not Found`             | In `server` terminology mode, a SNOMED CT implicit concept map URL (`http://snomed.info/sct?fhir_cm=...`) that the terminology server's ConceptMap search does not return; the issue adds that such maps are resolved only in local terminology mode.                                                                                                                                                                                                                                                                                                          |
| `404 Not Found`             | An unpinned concept map held in several versions with no determinable latest, or a pinned one held twice at that version; the issue states that the version to use cannot be determined, and why.                                                                                                                                                                                                                                                                                                                                                              |
| `422 Unprocessable Entity`  | The subject is of no admitted kind, or is conformant but cannot be processed (for example a column type `_format=fhir` cannot express).                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `422 Unprocessable Entity`  | A value set is found but its membership cannot be determined: a later page of its expansion fails, the terminology layer cannot expand a supplied ValueSet's `compose`, the local store cannot evaluate the compose, or a supplied ValueSet has neither an `expansion` nor a `compose`. The issue names the label, the canonical URL and the reason.                                                                                                                                                                                                           |
| `422 Unprocessable Entity`  | A value set's membership exceeds `pathling.sqlQuery.valueSetMaxMembers`; the issue names the label, the canonical URL and the maximum.                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `422 Unprocessable Entity`  | A supplied ValueSet's `expansion` is incomplete (carries an `offset`, or a `total` greater than its entry count) or has an entry that does not identify a member; the issue names `context`.                                                                                                                                                                                                                                                                                                                                                                   |
| `422 Unprocessable Entity`  | In `server` terminology mode, nothing resolves a dependency and at least one terminology lookup had an unknown outcome: `$expand` of the URL was answered with a failure status other than `404` before any part of an expansion was returned, or the ConceptMap search was answered with a `5xx` other than `501` or could not reach the server. There is one issue per failed lookup, value set first, each naming the label, the reference, the operation that failed and the server's reason, without calling the dependency a value set or a concept map. |
| `422 Unprocessable Entity`  | In `server` terminology mode, the terminology server cannot be reached while a dependency is looked up as a value set; the issue names the label, the reference and the configured server URL, and no ConceptMap search is attempted.                                                                                                                                                                                                                                                                                                                          |
| `422 Unprocessable Entity`  | A concept map is found but carries content the relation cannot represent (see [Concept maps](../sql-on-fhir.md#concept-maps)), or, in `server` terminology mode, cannot be read once chosen; the issue names the label, the canonical URL and the reason.                                                                                                                                                                                                                                                                                                      |
| `422 Unprocessable Entity`  | A concept map has more rows than `pathling.sqlQuery.conceptMapMaxMappings`; the issue names the label, the canonical URL and the maximum.                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `422 Unprocessable Entity`  | In `local` terminology mode, a SNOMED CT implicit concept map URL carrying a `\|version` pin, which cannot apply since such a URL carries its version in its base.                                                                                                                                                                                                                                                                                                                                                                                             |
| `500 Internal Server Error` | An unexpected execution or infrastructure fault.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `500 Internal Server Error` | A configured external table cannot be read; the issue names the table's `url` and never its storage path.                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

Every 4xx and 5xx response carries an
[OperationOutcome](https://hl7.org/fhir/R4/operationoutcome.html) whose issues
name the parameter at fault in `expression`. A request that is wrong in more
than one way is answered with one outcome naming every problem, so it can be
corrected in a single round trip.

An unbound [parameter](../sql-on-fhir.md#parameters) is a `400`, and a fault
in the subject's SQL that only Spark's analyser can catch is a `422`; see
[The SQL](../sql-on-fhir.md#the-sql).

A fault that only appears when rows are evaluated - for example a column
declared as singular that yields more than one value for some resource - is
reported the same way as it is by `$sql-export`: `400` for a failure raised by
the query itself, `500` for anything else. The result is streamed, so this is
only possible while no rows have been sent. Rows are evaluated in batches, and
the first batch is evaluated before anything is written; with `_limit`,
`_format=json` or `_format=parquet`, every row that could be returned is
evaluated first. Without `_limit`, a failure in a later batch can arrive after
the `200` status and the earlier rows. The server then closes the connection
without completing the response, so an HTTP/1.1 client reports an incomplete
transfer rather than a result that is silently missing rows. Use `$sql-export`
to have every failure reported as a status.

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
value set or concept map dependency requires only the operation authority. See
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
