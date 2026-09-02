# Search

Pathling supports two complementary approaches to searching FHIR resources:

1. **Standard FHIR search parameters** — filter using the search parameters defined in the FHIR specification (e.g., `gender`, `birthdate`, `code`).
2. **FHIRPath-based search** — filter using arbitrary [FHIRPath](/docs/fhirpath.md) expressions for more complex queries.

Both approaches can be combined in a single request.

## Standard search parameters[​](#standard-search-parameters "Direct link to Standard search parameters")

Standard FHIR search parameters can be used directly in the URL query string, following the [FHIR search specification](https://hl7.org/fhir/R4/search.html).

```
GET [FHIR endpoint]/[resource type]?[parameter]=[value]&...
```

### Supported parameter types[​](#supported-parameter-types "Direct link to Supported parameter types")

| Type      | Description                                       | Example                      |
| --------- | ------------------------------------------------- | ---------------------------- |
| Token     | Matches a code, system\|code, or boolean value    | `gender=male`                |
| String    | Case-insensitive prefix match on string fields    | `family=Smith`               |
| Date      | Matches date/dateTime values with optional prefix | `birthdate=ge1990-01-01`     |
| Number    | Matches numeric values with optional prefix       | `length=gt5`                 |
| Quantity  | Matches quantity values with optional units       | `value-quantity=gt100\|\|mg` |
| Reference | Matches references to other resources             | `subject=Patient/123`        |
| URI       | Matches URI values with exact equality            | `url=http://example.org`     |

### Supported modifiers[​](#supported-modifiers "Direct link to Supported modifiers")

| Modifier  | Applies to            | Description                                                       |
| --------- | --------------------- | ----------------------------------------------------------------- |
| `:not`    | Token, Reference, URI | Negated matching                                                  |
| `:exact`  | String                | Case-sensitive exact match (instead of prefix)                    |
| `:below`  | URI                   | Prefix matching on URI values                                     |
| `:above`  | URI                   | Inverse prefix matching on URI values                             |
| `:[type]` | Reference             | Constrains the target resource type (e.g., `subject:Patient=123`) |

### Combining parameters[​](#combining-parameters "Direct link to Combining parameters")

Following the FHIR specification:

* **AND logic**: Repeat the same or different parameters (`gender=male&active=true`).
* **OR logic**: Use comma-separated values within a single parameter (`gender=male,female`).

### Date prefixes[​](#date-prefixes "Direct link to Date prefixes")

Date parameters support comparison prefixes:

| Prefix | Meaning               |
| ------ | --------------------- |
| `eq`   | Equal (default)       |
| `ne`   | Not equal             |
| `gt`   | Greater than          |
| `lt`   | Less than             |
| `ge`   | Greater than or equal |
| `le`   | Less than or equal    |

### Examples[​](#examples "Direct link to Examples")

Retrieve all male patients:

```
GET /fhir/Patient?gender=male
```

Retrieve patients born on or after 1990:

```
GET /fhir/Patient?birthdate=ge1990-01-01
```

Retrieve patients with family name starting with "Smith":

```
GET /fhir/Patient?family=Smith
```

Retrieve patients with exact family name match:

```
GET /fhir/Patient?family:exact=Smith
```

Retrieve male patients born after 1980 (AND logic):

```
GET /fhir/Patient?gender=male&birthdate=gt1980-01-01
```

Retrieve male or female patients (OR logic):

```
GET /fhir/Patient?gender=male,female
```

## FHIRPath-based search[​](#fhirpath-based-search "Direct link to FHIRPath-based search")

For queries that go beyond what standard search parameters can express, Pathling supports FHIRPath-based search using a [named query](https://hl7.org/fhir/R4/search.html#advanced) called `fhirPath`.

Each FHIRPath expression is evaluated against each resource, returning a Boolean value which determines whether the resource will be included in the search result.

```
GET [FHIR endpoint]/[resource type]?_query=fhirPath&filter=[FHIRPath expression]...
```

```
POST [FHIR endpoint]/[resource type]/_search
```

### Request[​](#request "Direct link to Request")

The `filter` parameter is used to specify FHIRPath expressions:

* `filter [1..*]` - (string) A FHIRPath expression that can be evaluated against each resource in the data set to determine whether it is included within the result. The context is an individual resource of the type that the search is being invoked against. The expression must evaluate to a Boolean value.

Multiple instances of the `filter` parameter are combined using Boolean AND logic, and multiple expressions can be provided within a single parameter and delimited by commas to achieve OR logic. In addition to this, [FHIRPath boolean operators](https://hl7.org/fhirpath/#boolean-logic) can be used within expressions.

### Examples[​](#examples-1 "Direct link to Examples")

Retrieve all male patients:

```
GET /fhir/Patient?_query=fhirPath&filter=gender%3D'male'
```

This is the URL-encoded form of `gender='male'`.

Retrieve female patients who are active (AND logic via multiple `filter` parameters):

```
GET /fhir/Patient?_query=fhirPath&filter=gender%3D'female'&filter=active%3Dtrue
```

This is the URL-encoded form of `gender='female'` and `active=true`.

Retrieve patients born after 1980:

```
GET /fhir/Patient?_query=fhirPath&filter=birthDate%20%3E%20%401980-01-01
```

This is the URL-encoded form of `birthDate > @1980-01-01`.

Retrieve observations with a specific code and value above threshold:

```
GET /fhir/Observation?_query=fhirPath&filter=code.coding.code%3D'8867-4'%20and%20valueQuantity.value%20%3E%20100
```

This is the URL-encoded form of `code.coding.code='8867-4' and valueQuantity.value > 100`.

## Combining standard and FHIRPath search[​](#combining-standard-and-fhirpath-search "Direct link to Combining standard and FHIRPath search")

Standard search parameters can be used alongside FHIRPath filters in the same request by including `_query=fhirPath` with both `filter` and standard parameters. The results are filtered by both the standard parameters and the FHIRPath expressions (AND logic).

```
GET /fhir/Patient?_query=fhirPath&filter=active%3Dtrue&gender=male
```

This retrieves active male patients using a FHIRPath filter for `active` and a standard search parameter for `gender`.

## Response[​](#response "Direct link to Response")

Any search request in FHIR returns a [Bundle](https://hl7.org/fhir/R4/bundle.html) of matching resources.

Pagination links are included for responses which include a large number of resources. The number of resources returned in a single response is 100 by default — this can be altered using the `_count` parameter.

See [Search](https://hl7.org/fhir/R4/search.html) in the FHIR specification for more details.

Example response:

```
{
    "resourceType": "Bundle",
    "type": "searchset",
    "total": 2,
    "link": [
        {
            "relation": "self",
            "url": "http://localhost:8080/fhir/Patient?gender=male"
        }
    ],
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-1",
                "gender": "male",
                "birthDate": "1970-01-01"
            }
        },
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-2",
                "gender": "male",
                "birthDate": "1985-06-15"
            }
        }
    ]
}
```

## Error handling[​](#error-handling "Direct link to Error handling")

The following errors return HTTP 400 Bad Request with an `OperationOutcome` resource:

* **Unknown search parameters**: Using a parameter not defined for the resource type (e.g., `Patient?nonexistent=value`).
* **Unsupported modifiers**: Using a modifier not supported for the parameter type (e.g., `gender:exact=male` — `:exact` is only valid for string parameters).
* **Invalid FHIRPath expressions**: Syntax errors in FHIRPath filter expressions.

## Limitations[​](#limitations "Direct link to Limitations")

The following search parameters and features are not yet supported:

* Underscore-prefixed common search parameters: `_id`, `_lastUpdated`, `_tag`, `_profile`, `_security`, `_text`, `_content`, `_list`, `_has`, `_type`.
* Composite search parameter type.
* Special search parameter type.
* Chained parameters (e.g., `subject.name=Smith`).
* Reverse chained parameters (`_has`).
* Result sorting (`_sort`).
* Include parameters (`_include`, `_revinclude`).
