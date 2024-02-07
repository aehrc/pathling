---
description: A client library for the Pathling FHIR API, for use with JavaScript and TypeScript.
---

# pathling-client

[pathling-client](https://www.npmjs.com/package/pathling-client) is a client
library for the Pathling FHIR API, for use with JavaScript and TypeScript.

## Example usage

```typescript
import PathlingClient from 'pathling-client';

const client = new PathlingClient("https://demo.pathling.app/fhir");

// Invoke the import operation.
client.import({
    sources: [
        { resourceType: "Patient", url: "s3://somebucket/Patient.ndjson" },
        { resourceType: "Condition", url: "s3://somebucket/Condition.ndjson" },
    ]
}).then(result => console.log(result));

// Invoke the aggregate operation.
client.aggregate({
    subjectResource: "Patient",
    aggregations: ["count()"],
    groupings: [
        "reverseResolve(Condition.subject).code.coding" +
        ".where(subsumedBy(http://snomed.info/sct|73211009))"
    ]
}).then(result => console.log(result));

// Invoke the search operation.
client.search({
    subjectResource: "Patient",
    filters: [
        "(reverseResolve(Condition.subject).code.coding" +
        ".where($this.subsumedBy(http://snomed.info/sct|73211009))) " +
        "contains http://snomed.info/sct|427089005||'Diabetes from Cystic Fibrosis'"
    ],
}).then(result => console.log(result));

// Invoke the extract operation.
client.extract({
    subjectResource: "Patient",
    columns: [
        "id",
        "reverseResolve(Condition.subject).code.coding"
    ],
    filters: [
        "reverseResolve(Condition.subject).code" +
        ".subsumedBy(http://snomed.info/sct|73211009).anyTrue"
    ]
}).then(result => console.log(result));
```
