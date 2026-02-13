# Implementation notes

Guidance for implementing FHIR Bulk Data servers and clients.

## Contents

- [Server implementation](#server-implementation)
- [Client implementation](#client-implementation)
- [Group export](#group-export)
- [Type filtering](#type-filtering)
- [Partial manifests](#partial-manifests)
- [Use cases](#use-cases)

## Server implementation

### Capability statement

Declare bulk data support:

```json
{
    "resourceType": "CapabilityStatement",
    "instantiates": [
        "http://hl7.org/fhir/uv/bulkdata/CapabilityStatement/bulk-data"
    ],
    "rest": [
        {
            "mode": "server",
            "operation": [
                {
                    "name": "export",
                    "definition": "http://hl7.org/fhir/uv/bulkdata/OperationDefinition/export"
                }
            ]
        }
    ]
}
```

### Job management

Track export jobs with persistent storage:

```
Job {
  id: string
  status: "accepted" | "in-progress" | "complete" | "error"
  request: string (original request URL)
  transactionTime: instant
  progress: number (0-100)
  outputFiles: File[]
  errors: OperationOutcome[]
  expiresAt: instant
}
```

### File generation strategy

Options for generating output files:

1. **Stream to storage**: Write resources directly to cloud storage as they are processed
2. **Batch and write**: Collect resources in memory, write in batches
3. **Database export**: Use database-native export features where available

Considerations:

- File size limits (recommend 100MB-1GB per file)
- Resource count per file (useful for progress tracking)
- Compression support (gzip recommended)

### Status endpoint behaviour

```java
@GET
@Path("/bulk-status/{jobId}")
public Response getStatus(@PathParam("jobId") String jobId) {
    Job job = jobStore.get(jobId);

    if (job == null) {
        return Response.status(404).build();
    }

    switch (job.status) {
        case "in-progress":
            return Response.status(202)
                .header("X-Progress", job.progress + "%")
                .header("Retry-After", "120")
                .build();

        case "complete":
            return Response.ok(buildManifest(job))
                .header("Expires", job.expiresAt.toString())
                .build();

        case "error":
            return Response.status(500)
                .entity(job.errors.get(0))
                .build();
    }
}
```

### Resource filtering

When `_since` is provided, filter by `Resource.meta.lastUpdated`:

```sql
SELECT * FROM resources
WHERE resource_type IN ('Patient', 'Observation')
  AND last_updated > '2024-01-01T00:00:00Z'
  AND patient_id IN (SELECT id FROM patients WHERE ...)
```

### Authorisation filtering

Apply access controls based on the client's authorised scope:

```java
List<Resource> filterByAuthorization(List<Resource> resources, Token token) {
    return resources.stream()
        .filter(r -> token.hasScope("system/" + r.getResourceType() + ".read"))
        .collect(toList());
}
```

## Client implementation

### Polling strategy

Implement exponential backoff with jitter:

```python
import time
import random

def poll_status(status_url, headers):
    base_delay = 5
    max_delay = 300
    attempt = 0

    while True:
        response = requests.get(status_url, headers=headers)

        if response.status_code == 200:
            return response.json()  # Complete manifest

        if response.status_code == 202:
            # Calculate delay with exponential backoff and jitter
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                delay = int(retry_after)
            else:
                delay = min(base_delay * (2 ** attempt), max_delay)
                delay = delay + random.uniform(0, delay * 0.1)  # Add jitter

            progress = response.headers.get("X-Progress", "unknown")
            print(f"Export in progress: {progress}. Waiting {delay}s...")

            time.sleep(delay)
            attempt += 1
            continue

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            continue

        # Handle errors
        raise Exception(f"Export failed: {response.status_code}")
```

### Downloading files

Download with streaming and retry logic:

```python
import gzip
from pathlib import Path

def download_files(manifest, output_dir, headers):
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for i, file_info in enumerate(manifest["output"]):
        url = file_info["url"]
        resource_type = file_info["type"]
        output_path = Path(output_dir) / f"{resource_type}_{i}.ndjson"

        response = requests.get(
            url,
            headers={**headers, "Accept-Encoding": "gzip"},
            stream=True
        )

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Downloaded {resource_type}: {file_info.get('count', '?')} resources")
```

### Processing NDJSON

Process large files line by line:

```python
import json

def process_ndjson(file_path, handler):
    with open(file_path, "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                resource = json.loads(line)
                handler(resource)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON on line {line_num}: {e}")
```

### Complete client example

```python
class BulkDataClient:
    def __init__(self, base_url, token):
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/fhir+json",
            "Prefer": "respond-async"
        }

    def export(self, resource_types=None, since=None):
        # Build request URL
        url = f"{self.base_url}/Patient/$export"
        params = {}
        if resource_types:
            params["_type"] = ",".join(resource_types)
        if since:
            params["_since"] = since

        # Kick off export
        response = requests.get(url, params=params, headers=self.headers)
        if response.status_code != 202:
            raise Exception(f"Export kick-off failed: {response.status_code}")

        status_url = response.headers["Content-Location"]

        # Poll for completion
        manifest = poll_status(status_url, self.headers)

        # Download files
        download_files(manifest, "./export_output", self.headers)

        # Cleanup
        requests.delete(status_url, headers=self.headers)

        return manifest
```

## Group export

Group export retrieves data for a specific cohort of patients.

### Creating a group

```json
{
    "resourceType": "Group",
    "id": "diabetes-cohort",
    "type": "person",
    "actual": true,
    "characteristic": [
        {
            "code": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "73211009",
                        "display": "Diabetes mellitus"
                    }
                ]
            },
            "valueBoolean": true,
            "exclude": false
        }
    ],
    "member": [
        { "entity": { "reference": "Patient/p1" } },
        { "entity": { "reference": "Patient/p2" } }
    ]
}
```

### Characteristic-based groups

Servers may support dynamic groups based on characteristics:

```http
GET /Group/diabetes-cohort/$export?_type=Patient,Observation,MedicationRequest
```

The server evaluates the group membership at export time based on defined characteristics.

## Type filtering

The `_typeFilter` parameter applies FHIR search queries to filter exported resources.

### Syntax

```
_typeFilter=[ResourceType]?[search-params]
```

### Examples

Export only active medications:

```http
GET /Patient/$export?_type=MedicationRequest&_typeFilter=MedicationRequest?status=active
```

Export observations from the last year:

```http
GET /Patient/$export?_type=Observation&_typeFilter=Observation?date=ge2023-01-01
```

Multiple filters (comma-separated, applied as OR):

```http
GET /Patient/$export?_typeFilter=Observation?code=8867-4,Observation?code=8310-5
```

### Limitations

- `_include` and `_sort` are not supported in `_typeFilter`
- Servers document supported search parameters in their CapabilityStatement
- Some search parameters may be unavailable during bulk export

## Partial manifests

For very large exports, servers may return partial manifests with pagination.

### Enabling partial manifests

```http
GET /Patient/$export?allowPartialManifests=true
```

### Paginated response

```json
{
    "transactionTime": "2024-06-15T10:30:00Z",
    "request": "[original request]",
    "requiresAccessToken": true,
    "output": [{ "type": "Patient", "url": "...", "count": 10000 }],
    "link": [
        {
            "relation": "next",
            "url": "https://example.org/bulk-status/123?page=2"
        }
    ]
}
```

Follow the `next` link to retrieve additional file URLs. Continue until no `next` link is present.

## Use cases

### Clinical data exchange

Export patient data for care coordination:

```http
GET /Group/attributed-patients/$export
    ?_type=Patient,Condition,Observation,MedicationRequest,AllergyIntolerance
    &_since=2024-01-01T00:00:00Z
```

### Quality measure calculation

Export data for ACO quality reporting:

```http
GET /Group/aco-patients/$export
    ?_type=Patient,Encounter,Condition,Procedure,Observation
    &_typeFilter=Encounter?date=ge2024-01-01&date=le2024-12-31
```

### Research data sets

Export de-identified data for research:

```http
GET /Patient/$export
    ?_type=Patient,Observation,Condition
    &_elements=Patient.birthDate,Patient.gender,Observation.code,Observation.value
```

### Financial data export

Export claims and coverage data:

```http
GET /Patient/$export?_type=Patient,ExplanationOfBenefit,Coverage,Claim
```

### Terminology export

Export code systems and value sets:

```http
GET /$export?_type=CodeSystem,ValueSet
```
