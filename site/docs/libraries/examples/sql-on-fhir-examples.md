---
sidebar_position: 1
title: SQL on FHIR
description: Examples of running SQL on FHIR queries using the Pathling libraries.
---

# SQL on FHIR examples

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

[SQL on FHIR](https://sql-on-fhir.org/) is a specification that enables the
creation of tabular views from FHIR resources using a declarative approach. This
page provides comprehensive examples demonstrating all aspects of SQL on FHIR
view definitions using the Pathling libraries.

## Introduction

SQL on FHIR allows you to transform FHIR resources into flat, tabular structures
suitable for analytics, reporting, and data warehousing. A **ViewDefinition**
specifies how to extract, transform, and organise data from FHIR resources into
rows and columns.

### Key concepts

- **Resource**: The FHIR resource type that the view is based on (e.g.,
  `Patient`, `Observation`)
- **Select**: Defines the columns and their content using FHIRPath expressions
- **Where**: Optional filters to include only specific resources
- **Constants**: Reusable values that can be referenced in expressions
- **forEach/forEachOrNull**: Iteration constructs for handling collections

## Basic structure

Here's the simplest possible view definition:

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "id",
                    "path": "id"
                }
            ]
        }
    ]
}
```

## Examples

### Basic column selection

#### Simple patient demographics

This example extracts basic patient information into a flat table.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "id"
                },
                {
                    "name": "gender",
                    "path": "gender"
                },
                {
                    "name": "birth_date",
                    "path": "birthDate",
                    "type": "date"
                },
                {
                    "name": "active",
                    "path": "active",
                    "type": "boolean"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
# Execute the view
result <- data_source %>% ds_view(
        resource = "Patient",
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "id", type = "id"),
                                list(name = "gender", path = "gender", type = "code"),
                                list(name = "birth_date", path = "birthDate", type = "date"),
                                list(name = "active", path = "active", type = "boolean")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Execute the view
result = data_source.view(
    resource="Patient",
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "id", "type": "id"},
                {"name": "gender", "path": "gender", "type": "code"},
                {"name": "birth_date", "path": "birthDate", "type": "date"},
                {"name": "active", "path": "active", "type": "boolean"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "active": true,
    "gender": "female",
    "birthDate": "1990-05-15"
}
```

**Expected output:**

| patient_id | gender | birth_date | active |
|------------|--------|------------|--------|
| patient-1  | female | 1990-05-15 | true   |

**Explanation:**
This example demonstrates the most basic form of a SQL on FHIR view. Each column
directly maps to a simple field in the Patient resource using FHIRPath
expressions. The `type` attribute ensures proper data type handling.

---

### Working with names and collections

#### Patient names with basic extraction

This example demonstrates name element extraction using supported syntax.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "id",
                    "path": "getResourceKey()"
                },
                {
                    "name": "family_name",
                    "path": "name.first().family"
                },
                {
                    "name": "given_name",
                    "path": "name.first().given.first()"
                },
                {
                    "name": "name_use",
                    "path": "name.first().use"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Patient",
        select = list(
                list(
                        column = list(
                                list(name = "id", path = "getResourceKey()", type = "id"),
                                list(name = "full_name",
                                     path = "name.where(use = 'official').select(given.join(' ') + ' ' + family).first()",
                                     type = "string"),
                                list(name = "family_name",
                                     path = "name.where(use = 'official').family.first()",
                                     type = "string"),
                                list(name = "given_names",
                                     path = "name.where(use = 'official').given.join(' ').first()",
                                     type = "string")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Patient",
    select=[
        {
            "column": [
                {"name": "id", "path": "getResourceKey()"},
                {"name": "family_name", "path": "name.first().family"},
                {"name": "given_name", "path": "name.first().given.first()"},
                {"name": "name_use", "path": "name.first().use"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "name": [
        {
            "use": "usual",
            "family": "Smith",
            "given": [
                "Jane",
                "Marie"
            ]
        },
        {
            "use": "official",
            "family": "Johnson",
            "given": [
                "Jane",
                "M."
            ]
        }
    ]
}
```

**Expected output:**

| id        | full_name       | family_name | given_names |
|-----------|-----------------|-------------|-------------|
| patient-1 | Jane M. Johnson | Johnson     | Jane M.     |

**Key features:**

- Uses `getResourceKey()` function for consistent ID generation
- Demonstrates filtering with `where()` clause
- Shows string concatenation and the `join()` function
- Uses `first()` to handle collections and get single values

---

### Filtering with where clauses

#### Basic patient filtering

This example demonstrates filtering with where clauses using supported syntax.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "where": [
        {
            "path": "gender.exists()"
        }
    ],
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "id"
                },
                {
                    "name": "birth_date",
                    "path": "birthDate",
                    "type": "date"
                },
                {
                    "name": "gender",
                    "path": "gender"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Patient",
        where = list(
                list(path = "gender.exists()")
        ),
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "id"),
                                list(name = "birth_date", path = "birthDate", type = "date"),
                                list(name = "gender", path = "gender")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Patient",
    where=[
        {"path": "gender.exists()"}
    ],
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "id"},
                {"name": "birth_date", "path": "birthDate", "type": "date"},
                {"name": "gender", "path": "gender"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
[
    {
        "resourceType": "Patient",
        "id": "patient-1",
        "gender": "female",
        "birthDate": "1985-03-15"
    },
    {
        "resourceType": "Patient",
        "id": "patient-2",
        "birthDate": "1990-07-22"
    },
    {
        "resourceType": "Patient",
        "id": "patient-3",
        "gender": "male",
        "birthDate": "2010-01-10"
    }
]
```

**Expected output:**

| patient_id | birth_date | gender |
|------------|------------|--------|
| patient-1  | 1985-03-15 | female |
| patient-3  | 2010-01-10 | male   |

**Explanation:**

- The `where` clause filters to include only patients who have a gender field
- Uses the `exists()` function to check for presence of the gender field
- Demonstrates basic filtering with where clauses
- Only patient-1 and patient-3 have gender information, so they are included

---

### Working with collections using forEach

#### Patient addresses

This example demonstrates how to handle one-to-many relationships using`forEach`
to create multiple rows per patient.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "getResourceKey()"
                }
            ]
        },
        {
            "forEach": "address",
            "column": [
                {
                    "name": "use",
                    "path": "use"
                },
                {
                    "name": "type",
                    "path": "type"
                },
                {
                    "name": "city",
                    "path": "city"
                },
                {
                    "name": "state",
                    "path": "state"
                },
                {
                    "name": "postal_code",
                    "path": "postalCode"
                },
                {
                    "name": "country",
                    "path": "country"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Patient",
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "getResourceKey()")
                        )
                ),
                list(
                        forEach = "address",
                        column = list(
                                list(name = "use", path = "use"),
                                list(name = "type", path = "type"),
                                list(name = "city", path = "city"),
                                list(name = "state", path = "state"),
                                list(name = "postal_code", path = "postalCode"),
                                list(name = "country", path = "country")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Patient",
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "getResourceKey()"}
            ]
        },
        {
            "forEach": "address",
            "column": [
                {"name": "use", "path": "use"},
                {"name": "type", "path": "type"},
                {"name": "city", "path": "city"},
                {"name": "state", "path": "state"},
                {"name": "postal_code", "path": "postalCode"},
                {"name": "country", "path": "country"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "address": [
        {
            "use": "home",
            "type": "both",
            "line": [
                "123 Main St",
                "Apt 4B"
            ],
            "city": "Boston",
            "state": "MA",
            "postalCode": "02101",
            "country": "US"
        },
        {
            "use": "work",
            "type": "both",
            "line": [
                "456 Work Ave"
            ],
            "city": "Boston",
            "state": "MA",
            "postalCode": "02102",
            "country": "US"
        }
    ]
}
```

**Expected output:**

| patient_id | use  | type | city   | state | postal_code | country |
|------------|------|------|--------|-------|-------------|---------|
| patient-1  | home | both | Boston | MA    | 02101       | US      |
| patient-1  | work | both | Boston | MA    | 02102       | US      |

**Key features:**

- `forEach` creates multiple rows (one per address) for each patient
- The first select provides patient-level columns that appear in every row
- The second select with `forEach` provides address-specific columns
- Simplified to avoid unsupported functions like `join()`

---

### Basic telecom information

#### Patient contact details

This example demonstrates basic telecom extraction without constants.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "id"
                }
            ]
        },
        {
            "forEach": "telecom",
            "column": [
                {
                    "name": "system",
                    "path": "system"
                },
                {
                    "name": "value",
                    "path": "value"
                },
                {
                    "name": "use",
                    "path": "use"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Patient",
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "id")
                        )
                ),
                list(
                        forEach = "telecom",
                        column = list(
                                list(name = "system", path = "system"),
                                list(name = "value", path = "value"),
                                list(name = "use", path = "use")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Patient",
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "id"}
            ]
        },
        {
            "forEach": "telecom",
            "column": [
                {"name": "system", "path": "system"},
                {"name": "value", "path": "value"},
                {"name": "use", "path": "use"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "telecom": [
        {
            "system": "phone",
            "value": "+1-617-555-1234",
            "use": "home"
        },
        {
            "system": "phone",
            "value": "+1-617-555-5678",
            "use": "work"
        },
        {
            "system": "email",
            "value": "john.doe@email.com",
            "use": "home"
        }
    ]
}
```

**Expected output:**

| patient_id | system | value              | use  |
|------------|--------|--------------------|------|
| patient-1  | phone  | +1-617-555-1234    | home |
| patient-1  | phone  | +1-617-555-5678    | work |
| patient-1  | email  | john.doe@email.com | home |

**Key features:**

- Uses `forEach` to create multiple rows for each telecom entry
- Demonstrates basic field extraction from telecom elements
- Simplified approach without constants or filtering

---

### Patient addresses (simplified)

#### Basic address extraction

This example demonstrates basic address extraction from patients using supported
syntax.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "getResourceKey()"
                }
            ]
        },
        {
            "forEach": "address",
            "column": [
                {
                    "name": "use",
                    "path": "use"
                },
                {
                    "name": "type",
                    "path": "type"
                },
                {
                    "name": "city",
                    "path": "city"
                },
                {
                    "name": "state",
                    "path": "state"
                },
                {
                    "name": "postal_code",
                    "path": "postalCode"
                },
                {
                    "name": "country",
                    "path": "country"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Patient",
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "getResourceKey()")
                        )
                ),
                list(
                        forEach = "address",
                        column = list(
                                list(name = "use", path = "use"),
                                list(name = "type", path = "type"),
                                list(name = "city", path = "city"),
                                list(name = "state", path = "state"),
                                list(name = "postal_code", path = "postalCode"),
                                list(name = "country", path = "country")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Patient",
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "getResourceKey()"}
            ]
        },
        {
            "forEach": "address",
            "column": [
                {"name": "use", "path": "use"},
                {"name": "type", "path": "type"},
                {"name": "city", "path": "city"},
                {"name": "state", "path": "state"},
                {"name": "postal_code", "path": "postalCode"},
                {"name": "country", "path": "country"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "address": [
        {
            "use": "home",
            "type": "both",
            "line": [
                "123 Main St",
                "Apt 4B"
            ],
            "city": "Boston",
            "state": "MA",
            "postalCode": "02101",
            "country": "US"
        },
        {
            "use": "work",
            "type": "both",
            "line": [
                "456 Work Ave"
            ],
            "city": "Cambridge",
            "state": "MA",
            "postalCode": "02139",
            "country": "US"
        }
    ]
}
```

**Expected output:**

| patient_id | use  | type | city      | state | postal_code | country |
|------------|------|------|-----------|-------|-------------|---------|
| patient-1  | home | both | Boston    | MA    | 02101       | US      |
| patient-1  | work | both | Cambridge | MA    | 02139       | US      |

**Key features:**

- `forEach` creates multiple rows (one per address) for each patient
- The first select provides patient-level columns that appear in every row
- The second select with `forEach` provides address-specific columns
- Simplified approach using basic address field extraction

---

### Basic observation data

This example demonstrates working with Observation resources using supported
syntax.

**View definition (JSON):**

```json
{
    "resource": "Observation",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "observation_id",
                    "path": "getResourceKey()"
                },
                {
                    "name": "patient_id",
                    "path": "subject.reference"
                },
                {
                    "name": "status",
                    "path": "status"
                },
                {
                    "name": "effective_datetime",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime"
                }
            ]
        },
        {
            "forEach": "code.coding",
            "column": [
                {
                    "name": "code_system",
                    "path": "system"
                },
                {
                    "name": "code_value",
                    "path": "code"
                },
                {
                    "name": "code_display",
                    "path": "display"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Observation",
        select = list(
                list(
                        column = list(
                                list(name = "observation_id", path = "getResourceKey()"),
                                list(name = "patient_id", path = "subject.reference"),
                                list(name = "status", path = "status"),
                                list(name = "effective_datetime", path = "effective.ofType(dateTime)", type = "dateTime")
                        )
                ),
                list(
                        forEach = "code.coding",
                        column = list(
                                list(name = "code_system", path = "system"),
                                list(name = "code_value", path = "code"),
                                list(name = "code_display", path = "display")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Observation",
    select=[
        {
            "column": [
                {"name": "observation_id", "path": "getResourceKey()"},
                {"name": "patient_id", "path": "subject.reference"},
                {"name": "status", "path": "status"},
                {"name": "effective_datetime",
                 "path": "effective.ofType(dateTime)", "type": "dateTime"}
            ]
        },
        {
            "forEach": "code.coding",
            "column": [
                {"name": "code_system", "path": "system"},
                {"name": "code_value", "path": "code"},
                {"name": "code_display", "path": "display"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Observation",
    "id": "obs-1",
    "status": "final",
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "72166-2",
                "display": "Tobacco smoking status"
            }
        ]
    },
    "subject": {
        "reference": "Patient/patient-1"
    },
    "effectiveDateTime": "2023-10-15T10:30:00Z"
}
```

**Expected output:**

| observation_id | patient_id        | status | effective_datetime  | code_system      | code_value | code_display           |
|----------------|-------------------|--------|---------------------|------------------|------------|------------------------|
| obs-1          | Patient/patient-1 | final  | 2023-10-15T10:30:00 | http://loinc.org | 72166-2    | Tobacco smoking status |

**Key features:**

- Extracts basic observation information using getResourceKey()
- Uses subject.reference to get patient reference
- Demonstrates `ofType()` to handle polymorphic effective field
- Uses `forEach` to iterate over observation code codings
- Simplified approach without complex filtering or constants

---

### Basic condition information

This example demonstrates working with Condition resources using supported
syntax.

**View definition (JSON):**

```json
{
    "resource": "Condition",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "condition_id",
                    "path": "getResourceKey()"
                },
                {
                    "name": "patient_id",
                    "path": "subject.reference"
                },
                {
                    "name": "encounter_id",
                    "path": "encounter.reference"
                },
                {
                    "name": "onset_datetime",
                    "path": "onset.ofType(dateTime)",
                    "type": "dateTime"
                },
                {
                    "name": "recorded_date",
                    "path": "recordedDate",
                    "type": "dateTime"
                }
            ]
        },
        {
            "forEach": "code.coding",
            "column": [
                {
                    "name": "code_system",
                    "path": "system"
                },
                {
                    "name": "code_value",
                    "path": "code"
                },
                {
                    "name": "code_display",
                    "path": "display"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Condition",
        select = list(
                list(
                        column = list(
                                list(name = "condition_id", path = "getResourceKey()"),
                                list(name = "patient_id", path = "subject.reference"),
                                list(name = "encounter_id", path = "encounter.reference"),
                                list(name = "onset_datetime", path = "onset.ofType(dateTime)", type = "dateTime"),
                                list(name = "recorded_date", path = "recordedDate", type = "dateTime")
                        )
                ),
                list(
                        forEach = "code.coding",
                        column = list(
                                list(name = "code_system", path = "system"),
                                list(name = "code_value", path = "code"),
                                list(name = "code_display", path = "display")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Condition",
    select=[
        {
            "column": [
                {"name": "condition_id", "path": "getResourceKey()"},
                {"name": "patient_id", "path": "subject.reference"},
                {"name": "encounter_id", "path": "encounter.reference"},
                {"name": "onset_datetime", "path": "onset.ofType(dateTime)",
                 "type": "dateTime"},
                {"name": "recorded_date", "path": "recordedDate",
                 "type": "dateTime"}
            ]
        },
        {
            "forEach": "code.coding",
            "column": [
                {"name": "code_system", "path": "system"},
                {"name": "code_value", "path": "code"},
                {"name": "code_display", "path": "display"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Condition",
    "id": "condition-1",
    "code": {
        "coding": [
            {
                "system": "http://snomed.info/sct",
                "code": "73211009",
                "display": "Diabetes mellitus"
            }
        ]
    },
    "subject": {
        "reference": "Patient/patient-1"
    },
    "encounter": {
        "reference": "Encounter/enc-1"
    },
    "onsetDateTime": "2023-01-15T09:00:00Z",
    "recordedDate": "2023-01-15T10:00:00Z"
}
```

**Expected output:**

| condition_id | patient_id        | encounter_id    | onset_datetime      | recorded_date       | code_system            | code_value | code_display      |
|--------------|-------------------|-----------------|---------------------|---------------------|------------------------|------------|-------------------|
| condition-1  | Patient/patient-1 | Encounter/enc-1 | 2023-01-15T09:00:00 | 2023-01-15T10:00:00 | http://snomed.info/sct | 73211009   | Diabetes mellitus |

**Key features:**

- Extracts basic condition information using getResourceKey()
- Uses subject.reference and encounter.reference to get references
- Demonstrates `ofType()` to handle polymorphic onset field (dateTime, Age,
  Period, etc.)
- Uses `forEach` to iterate over condition code codings
- Simplified approach without complex status handling or categories

---

### Basic patient extensions

#### Extension field extraction

This example demonstrates extension field extraction using supported syntax.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "getResourceKey()"
                }
            ]
        },
        {
            "forEach": "extension",
            "column": [
                {
                    "name": "extension_url",
                    "path": "url"
                },
                {
                    "name": "extension_value_string",
                    "path": "value.ofType(string)"
                },
                {
                    "name": "extension_value_code",
                    "path": "value.ofType(code)"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Patient",
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "getResourceKey()")
                        )
                ),
                list(
                        forEach = "extension",
                        column = list(
                                list(name = "extension_url", path = "url"),
                                list(name = "extension_value_string", path = "value.ofType(string)"),
                                list(name = "extension_value_code", path = "value.ofType(code)")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Patient",
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "getResourceKey()"}
            ]
        },
        {
            "forEach": "extension",
            "column": [
                {"name": "extension_url", "path": "url"},
                {"name": "extension_value_string",
                 "path": "value.ofType(string)"},
                {"name": "extension_value_code", "path": "value.ofType(code)"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "extension": [
        {
            "url": "http://example.org/fhir/StructureDefinition/preferred-language",
            "valueCode": "en"
        },
        {
            "url": "http://example.org/fhir/StructureDefinition/notes",
            "valueString": "Patient prefers morning appointments"
        }
    ]
}
```

**Expected output:**

| patient_id | extension_url                                                  | extension_value_string               | extension_value_code |
|------------|----------------------------------------------------------------|--------------------------------------|----------------------|
| patient-1  | http://example.org/fhir/StructureDefinition/preferred-language | null                                 | en                   |
| patient-1  | http://example.org/fhir/StructureDefinition/notes              | Patient prefers morning appointments | null                 |

**Key features:**

- Uses `forEach` to create multiple rows for each extension
- Demonstrates basic extension field extraction using getResourceKey()
- Uses `ofType()` to handle polymorphic extension values (string, code, etc.)
- Simplified approach without complex nested extensions or constants

---

### Basic encounter information

#### Encounter details extraction

This example demonstrates working with Encounter resources using supported
syntax.

**View definition (JSON):**

```json
{
    "resource": "Encounter",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "encounter_id",
                    "path": "getResourceKey()"
                },
                {
                    "name": "patient_id",
                    "path": "subject.reference"
                },
                {
                    "name": "encounter_class_system",
                    "path": "class.system"
                },
                {
                    "name": "encounter_class_code",
                    "path": "class.code"
                },
                {
                    "name": "encounter_status",
                    "path": "status"
                },
                {
                    "name": "period_start",
                    "path": "period.start",
                    "type": "dateTime"
                },
                {
                    "name": "period_end",
                    "path": "period.end",
                    "type": "dateTime"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Encounter",
        select = list(
                list(
                        column = list(
                                list(name = "encounter_id", path = "getResourceKey()"),
                                list(name = "patient_id", path = "subject.reference"),
                                list(name = "encounter_class_system", path = "class.system"),
                                list(name = "encounter_class_code", path = "class.code"),
                                list(name = "encounter_status", path = "status"),
                                list(name = "period_start", path = "period.start", type = "dateTime"),
                                list(name = "period_end", path = "period.end", type = "dateTime")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Encounter",
    select=[
        {
            "column": [
                {"name": "encounter_id", "path": "getResourceKey()"},
                {"name": "patient_id", "path": "subject.reference"},
                {"name": "encounter_class_system", "path": "class.system"},
                {"name": "encounter_class_code", "path": "class.code"},
                {"name": "encounter_status", "path": "status"},
                {"name": "period_start", "path": "period.start",
                 "type": "dateTime"},
                {"name": "period_end", "path": "period.end", "type": "dateTime"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Encounter",
    "id": "enc-1",
    "status": "finished",
    "class": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "code": "AMB",
        "display": "ambulatory"
    },
    "subject": {
        "reference": "Patient/patient-1"
    },
    "period": {
        "start": "2023-10-15T09:00:00Z",
        "end": "2023-10-15T10:30:00Z"
    }
}
```

**Expected output:**

| encounter_id | patient_id        | encounter_class_system                           | encounter_class_code | encounter_status | period_start        | period_end          |
|--------------|-------------------|--------------------------------------------------|----------------------|------------------|---------------------|---------------------|
| enc-1        | Patient/patient-1 | http://terminology.hl7.org/CodeSystem/v3-ActCode | AMB                  | finished         | 2023-10-15T09:00:00 | 2023-10-15T10:30:00 |

**Key features:**

- Extracts basic encounter information using getResourceKey()
- Uses subject.reference to get patient reference
- Shows how to handle encounter class (Coding datatype) with system and code
- Demonstrates extracting period start/end dates with dateTime type
- Simplified approach without complex diagnosis handling or reference extraction

---

### Basic telecom extraction

#### Patient telecom with boolean flag

This example demonstrates telecom field extraction with forEach.

**View definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "getResourceKey()"
                }
            ]
        },
        {
            "forEach": "telecom",
            "column": [
                {
                    "name": "system",
                    "path": "system"
                },
                {
                    "name": "value",
                    "path": "value"
                },
                {
                    "name": "use",
                    "path": "use"
                },
                {
                    "name": "has_phone",
                    "path": "system = 'phone'",
                    "type": "boolean"
                }
            ]
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Patient",
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "getResourceKey()")
                        )
                ),
                list(
                        forEach = "telecom",
                        column = list(
                                list(name = "system", path = "system"),
                                list(name = "value", path = "value"),
                                list(name = "use", path = "use"),
                                list(name = "has_phone", path = "system = 'phone'", type = "boolean")
                        )
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Patient",
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "getResourceKey()"}
            ]
        },
        {
            "forEach": "telecom",
            "column": [
                {"name": "system", "path": "system"},
                {"name": "value", "path": "value"},
                {"name": "use", "path": "use"},
                {"name": "has_phone", "path": "system = 'phone'",
                 "type": "boolean"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "telecom": [
        {
            "system": "phone",
            "value": "+1-617-555-1234",
            "use": "home"
        },
        {
            "system": "email",
            "value": "patient@email.com",
            "use": "home"
        }
    ]
}
```

**Expected output:**

| patient_id | system | value | use | has_phone |
|------------|--------|---------------------|------|-----------||
| patient-1 | phone | +1-617-555-1234 | home | true |
| patient-1 | email | patient@email.com | home | false |

**Key features:**

- Uses `forEach` to create multiple rows for each telecom entry
- Demonstrates basic telecom field extraction using getResourceKey()
- Shows boolean expression evaluation with `system = 'phone'`
- Simplified approach focusing on basic telecom iteration and boolean flags

---

## Best practices and tips

- Use `where` clauses to filter early and reduce processing
- Consider using `forEachOrNull` instead of `forEach` when you need consistent
  row counts
- Use constants for frequently referenced values like code systems
- Use descriptive column names and include descriptions for each expression
- Handle missing data explicitly with null checks
- Use `ofType()` when dealing with polymorphic fields
- Consider using `first()` or other collection functions to handle unexpected
  multiples
- `getResourceKey()` for consistent resource identification
- `getReferenceKey(ResourceType)` for extracting reference IDs
- `.coding.where(system = 'url').code` for extracting specific codes
- `.join(' ')` for combining arrays into strings

## Error handling

If your view definition contains errors, Pathling will provide detailed error
messages. Common issues include:

1. **Invalid FHIRPath expressions**: Check syntax and function usage
2. **Type mismatches**: Ensure column types match the data being extracted
3. **Missing constants**: Ensure all referenced constants are defined

## Further reading

- [SQL on FHIR Specification](https://sql-on-fhir.org/)
- [FHIRPath Specification](http://hl7.org/fhirpath/)
