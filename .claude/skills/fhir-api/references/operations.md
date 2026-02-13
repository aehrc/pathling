# FHIR REST operations

Detailed specifications for FHIR RESTful operations.

## Contents

- [Read](#read)
- [VRead](#vread)
- [Create](#create)
- [Update](#update)
- [Patch](#patch)
- [Delete](#delete)
- [History](#history)
- [Capabilities](#capabilities)

## Read

Retrieve the current version of a resource.

**Request:** `GET [base]/[type]/[id]`

**Response:**

- `200 OK` with resource body
- `404 Not Found` if resource does not exist
- `410 Gone` if resource was deleted

**Headers:**

- Response includes `ETag` and `Last-Modified`
- Use `If-None-Match` or `If-Modified-Since` for conditional reads (returns `304 Not Modified`)

**Example:**

```http
GET /Patient/123 HTTP/1.1
Accept: application/fhir+json
```

## VRead

Retrieve a specific version of a resource.

**Request:** `GET [base]/[type]/[id]/_history/[vid]`

**Response:**

- `200 OK` with resource body at that version
- `404 Not Found` if version does not exist

The version ID comes from `meta.versionId` or the ETag header.

## Create

Create a new resource with server-assigned ID.

**Request:** `POST [base]/[type]`

**Request body:** Resource without `id` (server ignores any provided `id`, `meta.versionId`, `meta.lastUpdated`)

**Response:**

- `201 Created` with `Location` header pointing to new resource
- `400 Bad Request` for invalid resource
- `422 Unprocessable Entity` for business rule violations

**Headers:**

- `Location: [base]/[type]/[id]/_history/[vid]`
- `ETag: W/"[vid]"`

### Conditional create

Prevent duplicate creation using `If-None-Exist` header with search parameters.

**Request:**

```http
POST /Patient HTTP/1.1
If-None-Exist: identifier=http://example.org|12345
Content-Type: application/fhir+json
```

**Behaviour:**

- No matches: Create resource, return `201 Created`
- One match: Return `200 OK` (no creation, idempotent)
- Multiple matches: Return `412 Precondition Failed`

## Update

Replace an existing resource entirely.

**Request:** `PUT [base]/[type]/[id]`

**Request body:** Complete resource with `id` matching URL

**Response:**

- `200 OK` for update
- `201 Created` if server allows upsert (update-as-create)
- `400 Bad Request` if `id` does not match URL
- `404 Not Found` if resource does not exist and upsert disabled
- `412 Precondition Failed` for version conflict

**Headers:**

- Include `If-Match: W/"[vid]"` for version-aware updates
- Response includes new `ETag` and `Location`

### Conditional update

Update based on search criteria instead of ID.

**Request:** `PUT [base]/[type]?[search-params]`

**Behaviour:**

- No matches, no ID in body: Create with server-assigned ID
- No matches, ID in body: Create with provided ID (if allowed)
- One match: Update that resource
- Multiple matches: Return `412 Precondition Failed`

### Update-as-create (upsert)

Some servers allow creating resources with client-defined IDs via PUT:

```http
PUT /Patient/my-custom-id HTTP/1.1
Content-Type: application/fhir+json

{"resourceType": "Patient", "id": "my-custom-id", ...}
```

Check `CapabilityStatement.rest.resource.updateCreate` to confirm support.

## Patch

Apply partial modifications to a resource.

**Request:** `PATCH [base]/[type]/[id]`

**Content types:**

| Format                | Content-Type                                  |
| --------------------- | --------------------------------------------- |
| JSON Patch (RFC 6902) | `application/json-patch+json`                 |
| XML Patch (RFC 5261)  | `application/xml-patch+xml`                   |
| FHIRPath Patch        | `application/fhir+json` (Parameters resource) |

### JSON Patch example

```http
PATCH /Patient/123 HTTP/1.1
Content-Type: application/json-patch+json
If-Match: W/"1"

[
  {"op": "replace", "path": "/birthDate", "value": "1990-01-15"},
  {"op": "add", "path": "/telecom/-", "value": {"system": "phone", "value": "555-1234"}}
]
```

### FHIRPath Patch example

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "operation",
            "part": [
                { "name": "type", "valueCode": "replace" },
                { "name": "path", "valueString": "Patient.birthDate" },
                { "name": "value", "valueDate": "1990-01-15" }
            ]
        }
    ]
}
```

### Conditional patch

**Request:** `PATCH [base]/[type]?[search-params]`

- One match: Apply patch
- No matches: Return `404 Not Found`
- Multiple matches: Return `412 Precondition Failed`

## Delete

Remove a resource.

**Request:** `DELETE [base]/[type]/[id]`

**Request body:** Empty

**Response:**

- `200 OK` with OperationOutcome
- `204 No Content` (no body)
- `202 Accepted` for async deletion
- `404 Not Found` or `200 OK` if already deleted (server choice)

Subsequent reads return `410 Gone`. Deleted resources appear in history with `request.method = DELETE`.

### Conditional delete

**Request:** `DELETE [base]/[type]?[search-params]`

**Behaviour:**

- No matches: Return `404 Not Found` or `204 No Content`
- One match: Delete resource
- Multiple matches: Server may delete all or return `412 Precondition Failed`

Check `CapabilityStatement.rest.resource.conditionalDelete` for server behaviour.

## History

Retrieve version history for a resource, type, or entire system.

**Scope:**

- Instance: `GET [base]/[type]/[id]/_history`
- Type: `GET [base]/[type]/_history`
- System: `GET [base]/_history`

**Parameters:**

- `_since=[instant]`: Only versions after this time
- `_at=[date]`: Versions at a specific date/range
- `_count=[n]`: Maximum entries per page
- `_sort`: `_lastUpdated` (oldest first), `-_lastUpdated` (newest first), or `none`

**Response:** Bundle with `type = history`, sorted oldest-to-newest by default.

Each entry includes:

- `resource`: The resource at that version (if not deleted)
- `request.method`: The operation (POST, PUT, DELETE)
- `request.url`: The request URL
- `response.status`: HTTP status code
- `response.lastModified`: Modification time

## Capabilities

Retrieve server capability statement.

**Request:** `GET [base]/metadata`

**Parameters:**

- `mode=full`: Complete CapabilityStatement (default)
- `mode=normative`: Normative portions only
- `mode=terminology`: TerminologyCapabilities resource

**Response:** CapabilityStatement describing supported:

- Resource types and operations
- Search parameters
- Versioning behaviour
- Conditional operations
- Extensions and profiles

The CapabilityStatement should include an ETag for caching.
