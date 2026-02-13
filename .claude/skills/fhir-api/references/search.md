# FHIR search operations

Detailed specifications for FHIR search functionality.

## Contents

- [Search methods](#search-methods)
- [Search parameters](#search-parameters)
- [Parameter types](#parameter-types)
- [Modifiers](#modifiers)
- [Prefixes](#prefixes)
- [Chaining](#chaining)
- [Includes](#includes)
- [Result control](#result-control)
- [Response format](#response-format)

## Search methods

FHIR search supports both GET and POST:

### GET search

```http
GET [base]/Patient?name=smith&birthdate=1990-01-01
```

### POST search

```http
POST [base]/Patient/_search
Content-Type: application/x-www-form-urlencoded

name=smith&birthdate=1990-01-01
```

Servers supporting search must support both methods but may return `405 Method Not Allowed` for one (not both).

### Search scopes

| Scope       | URL                           | Description                 |
| ----------- | ----------------------------- | --------------------------- |
| Type        | `[base]/[type]?params`        | Search within resource type |
| Compartment | `[base]/Patient/123/*?params` | Search within compartment   |
| System      | `[base]?params`               | Search across all resources |

## Search parameters

### Common parameters

| Parameter      | Description                   |
| -------------- | ----------------------------- |
| `_id`          | Logical resource ID           |
| `_lastUpdated` | Last modification time        |
| `_tag`         | Resource tags                 |
| `_profile`     | Conformance profile           |
| `_security`    | Security labels               |
| `_text`        | Full-text search on narrative |
| `_content`     | Full-text search on content   |
| `_list`        | Search within a List          |

### Resource-specific parameters

Each resource type defines specific search parameters. Check CapabilityStatement for supported parameters.

Example Patient parameters:

- `name`: Patient name
- `identifier`: Patient identifier
- `birthdate`: Date of birth
- `gender`: Gender
- `address`: Address
- `telecom`: Contact details

## Parameter types

### String

Matches start of string, case-insensitive:

```
name=eve           # Matches "Eve", "Evelyn"
name:exact=Eve     # Exact match only
name:contains=vel  # Contains substring
```

### Token

For coded values and identifiers:

```
identifier=12345                    # Value only
identifier=http://example.org|123   # System and value
identifier=http://example.org|      # System only
identifier=|123                     # Value only, any system
```

### Reference

For resource references:

```
subject=Patient/123                           # Relative reference
subject=https://example.org/fhir/Patient/123  # Absolute reference
subject:identifier=http://example.org|12345   # By identifier
```

### Date

Date/time values with precision handling:

```
birthdate=1990-01-15           # Exact date
birthdate=1990-01                # Any date in January 1990
birthdate=1990                   # Any date in 1990
```

### Quantity

Numeric values with optional units:

```
value-quantity=5.4|http://unitsofmeasure.org|mg
value-quantity=5.4||mg           # Code only
value-quantity=5.4               # Number only
```

### URI

Exact URI matching:

```
url=http://example.org/fhir/ValueSet/my-valueset
url:below=http://example.org/fhir/  # URI prefix
```

### Composite

Multiple parameters combined:

```
component-code-value-quantity=http://loinc.org|8480-6$lt100
```

## Modifiers

Modifiers alter parameter behaviour:

| Modifier      | Applicable to | Description                |
| ------------- | ------------- | -------------------------- |
| `:missing`    | All           | `true` if element absent   |
| `:exact`      | String        | Case-sensitive exact match |
| `:contains`   | String        | Substring match            |
| `:text`       | Token         | Match on display text      |
| `:not`        | Token         | Negation                   |
| `:above`      | Token, URI    | Concept hierarchy above    |
| `:below`      | Token, URI    | Concept hierarchy below    |
| `:in`         | Token         | Member of ValueSet         |
| `:not-in`     | Token         | Not member of ValueSet     |
| `:of-type`    | Token         | Identifier type            |
| `:identifier` | Reference     | Match by identifier        |
| `:[type]`     | Reference     | Limit reference type       |

## Prefixes

Numeric and date comparisons:

| Prefix | Meaning          |
| ------ | ---------------- |
| `eq`   | Equal (default)  |
| `ne`   | Not equal        |
| `gt`   | Greater than     |
| `lt`   | Less than        |
| `ge`   | Greater or equal |
| `le`   | Less or equal    |
| `sa`   | Starts after     |
| `eb`   | Ends before      |
| `ap`   | Approximately    |

Example:

```
birthdate=ge1990-01-01&birthdate=lt2000-01-01
```

## Chaining

Search by properties of referenced resources:

```
# Observations for patients named "Smith"
Observation?subject:Patient.name=smith

# Observations for patients in organisation "Acme"
Observation?subject:Patient.organization.name=acme
```

### Reverse chaining

Find resources referenced by others using `_has`:

```
# Patients with Observations coded as blood pressure
Patient?_has:Observation:subject:code=http://loinc.org|85354-9
```

## Includes

Include referenced resources in results:

### Forward includes

```
# Include referenced Practitioners
MedicationRequest?_include=MedicationRequest:requester
```

### Reverse includes

```
# Include Observations referencing returned Patients
Patient?_revinclude=Observation:subject
```

### Iterative includes

```
# Recursively include references
Patient?_include:iterate=*
```

## Result control

### Paging

| Parameter | Description                         |
| --------- | ----------------------------------- |
| `_count`  | Results per page (server may limit) |
| `_offset` | Skip first N results                |

Response Bundle includes pagination links:

- `self`: Current page
- `first`: First page
- `previous`: Previous page
- `next`: Next page
- `last`: Last page

### Sorting

```
_sort=birthdate           # Ascending
_sort=-birthdate          # Descending
_sort=family,-birthdate   # Multiple fields
```

### Summary

| Value            | Description               |
| ---------------- | ------------------------- |
| `_summary=true`  | Summary elements only     |
| `_summary=text`  | Text, id, meta, mandatory |
| `_summary=data`  | All except text           |
| `_summary=count` | Count only, no resources  |
| `_summary=false` | Full resources (default)  |

### Elements

Return specific elements only:

```
_elements=id,name,birthDate
```

### Total

| Value             | Description     |
| ----------------- | --------------- |
| `_total=none`     | No total        |
| `_total=estimate` | Estimated total |
| `_total=accurate` | Exact count     |

## Response format

Successful search returns `200 OK` with Bundle:

```json
{
  "resourceType": "Bundle",
  "type": "searchset",
  "total": 42,
  "link": [
    {"relation": "self", "url": "[base]/Patient?name=smith"},
    {"relation": "next", "url": "[base]/Patient?name=smith&_page=2"}
  ],
  "entry": [
    {
      "fullUrl": "[base]/Patient/123",
      "resource": {"resourceType": "Patient", ...},
      "search": {
        "mode": "match",
        "score": 0.95
      }
    }
  ]
}
```

### Entry modes

| Mode      | Description                           |
| --------- | ------------------------------------- |
| `match`   | Matched search criteria               |
| `include` | Included via `_include`/`_revinclude` |
| `outcome` | OperationOutcome with warnings        |

Empty results return `200 OK` with empty Bundle (`total: 0`).
