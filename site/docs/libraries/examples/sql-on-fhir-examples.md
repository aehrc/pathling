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

### Key Concepts

- **Resource**: The FHIR resource type that the view is based on (e.g.,
  `Patient`, `Observation`)
- **Select**: Defines the columns and their content using FHIRPath expressions
- **Where**: Optional filters to include only specific resources
- **Constants**: Reusable values that can be referenced in expressions
- **forEach/forEachOrNull**: Iteration constructs for handling collections

## Basic Structure

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

**View Definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "id",
                    "type": "id"
                },
                {
                    "name": "gender",
                    "path": "gender",
                    "type": "code"
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

**Sample Input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "active": true,
    "gender": "female",
    "birthDate": "1990-05-15"
}
```

**Expected Output:**
| patient_id | gender | birth_date | active |
|------------|--------|------------|---------|
| patient-1 | female | 1990-05-15 | true |

**Explanation:**
This example demonstrates the most basic form of a SQL on FHIR view. Each column
directly maps to a simple field in the Patient resource using FHIRPath
expressions. The `type` attribute ensures proper data type handling.

---

### Working with names and collections

#### Patient names with string functions

This example shows how to work with complex data types and use FHIRPath string
functions.

**View Definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "id",
                    "path": "getResourceKey()",
                    "type": "id"
                },
                {
                    "name": "full_name",
                    "path": "name.where(use = 'official').select(given.join(' ') + ' ' + family).first()",
                    "type": "string",
                    "description": "Full name from official name, given names joined with spaces"
                },
                {
                    "name": "family_name",
                    "path": "name.where(use = 'official').family.first()",
                    "type": "string"
                },
                {
                    "name": "given_names",
                    "path": "name.where(use = 'official').given.join(' ').first()",
                    "type": "string"
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
                {"name": "id", "path": "getResourceKey()", "type": "id"},
                {"name": "full_name",
                 "path": "name.where(use = 'official').select(given.join(' ') + ' ' + family).first()",
                 "type": "string"},
                {"name": "family_name",
                 "path": "name.where(use = 'official').family.first()",
                 "type": "string"},
                {"name": "given_names",
                 "path": "name.where(use = 'official').given.join(' ').first()",
                 "type": "string"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample Input:**

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

**Expected Output:**
| id | full_name | family_name | given_names |
|-----------|----------------|-------------|-------------|
| patient-1 | Jane M. Johnson| Johnson | Jane M. |

**Key Features:**

- Uses `getResourceKey()` function for consistent ID generation
- Demonstrates filtering with `where()` clause
- Shows string concatenation and the `join()` function
- Uses `first()` to handle collections and get single values

---

### Filtering with where clauses

#### Active adult patients

This example shows how to use where clauses to filter which resources are
included in the view.

**View Definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "where": [
        {
            "path": "active = true"
        },
        {
            "path": "birthDate <= today() - 18 years"
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
                    "name": "age_years",
                    "path": "(today() - birthDate).days() / 365.25",
                    "type": "decimal",
                    "description": "Calculated age in years"
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
                list(path = "active = true"),
                list(path = "birthDate <= today() - 18 years")
        ),
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "id"),
                                list(name = "age_years",
                                     path = "(today() - birthDate).days() / 365.25",
                                     type = "decimal"),
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
        {"path": "active = true"},
        {"path": "birthDate <= today() - 18 years"}
    ],
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "id"},
                {"name": "age_years",
                 "path": "(today() - birthDate).days() / 365.25",
                 "type": "decimal"},
                {"name": "gender", "path": "gender"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Explanation:**

- The `where` clauses filter to include only active patients who are 18 or older
- Multiple where conditions are combined with AND logic
- Demonstrates date arithmetic with `today()` and time duration calculations
- Shows how to calculate derived values (age) in the select clause

---

### Working with collections using forEach

#### Patient addresses

This example demonstrates how to handle one-to-many relationships using`forEach`
to create multiple rows per patient.

**View Definition (JSON):**

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
                    "path": "use",
                    "type": "code"
                },
                {
                    "name": "type",
                    "path": "type",
                    "type": "code"
                },
                {
                    "name": "street",
                    "path": "line.join('\\n')",
                    "type": "string",
                    "description": "Street address lines joined with newlines"
                },
                {
                    "name": "city",
                    "path": "city",
                    "type": "string"
                },
                {
                    "name": "state",
                    "path": "state",
                    "type": "string"
                },
                {
                    "name": "postal_code",
                    "path": "postalCode",
                    "type": "string"
                },
                {
                    "name": "country",
                    "path": "country",
                    "type": "string"
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
                                list(name = "use", path = "use", type = "code"),
                                list(name = "type", path = "type", type = "code"),
                                list(name = "street", path = "line.join('\\n')", type = "string"),
                                list(name = "city", path = "city", type = "string"),
                                list(name = "state", path = "state", type = "string"),
                                list(name = "postal_code", path = "postalCode", type = "string"),
                                list(name = "country", path = "country", type = "string")
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
                {"name": "use", "path": "use", "type": "code"},
                {"name": "type", "path": "type", "type": "code"},
                {"name": "street", "path": "line.join('\\n')",
                 "type": "string"},
                {"name": "city", "path": "city", "type": "string"},
                {"name": "state", "path": "state", "type": "string"},
                {"name": "postal_code", "path": "postalCode", "type": "string"},
                {"name": "country", "path": "country", "type": "string"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample Input:**

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

**Expected Output:**
| patient_id | use | type | street | city | state | postal_code | country |
|------------|------|------|------------------|--------|-------|-------------|---------|
| patient-1 | home | both | 123 Main St\nApt 4B | Boston | MA | 02101 | US |
| patient-1 | work | both | 456 Work Ave | Boston | MA | 02102 | US |

**Key Features:**

- `forEach` creates multiple rows (one per address) for each patient
- The first select provides patient-level columns that appear in every row
- The second select with `forEach` provides address-specific columns
- Uses `line.join('\\n')` to combine multiple address lines

---

### Using constants for reusable values

#### Filtered contact information

This example shows how to use constants to make view definitions more
maintainable and readable.

**View Definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "constant": [
        {
            "name": "phone_system",
            "valueCode": "phone"
        },
        {
            "name": "email_system",
            "valueCode": "email"
        },
        {
            "name": "home_use",
            "valueCode": "home"
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
                    "name": "home_phone",
                    "path": "telecom.where(system = %phone_system and use = %home_use).value.first()",
                    "type": "string"
                },
                {
                    "name": "home_email",
                    "path": "telecom.where(system = %email_system and use = %home_use).value.first()",
                    "type": "string"
                },
                {
                    "name": "any_phone",
                    "path": "telecom.where(system = %phone_system).value.first()",
                    "type": "string",
                    "description": "First phone number regardless of use"
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
        constants = list(
                list(name = "phone_system", valueCode = "phone"),
                list(name = "email_system", valueCode = "email"),
                list(name = "home_use", valueCode = "home")
        ),
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "id"),
                                list(name = "home_phone",
                                     path = "telecom.where(system = %phone_system and use = %home_use).value.first()",
                                     type = "string"),
                                list(name = "home_email",
                                     path = "telecom.where(system = %email_system and use = %home_use).value.first()",
                                     type = "string"),
                                list(name = "any_phone",
                                     path = "telecom.where(system = %phone_system).value.first()",
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
    constants=[
        {"name": "phone_system", "valueCode": "phone"},
        {"name": "email_system", "valueCode": "email"},
        {"name": "home_use", "valueCode": "home"}
    ],
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "id"},
                {"name": "home_phone",
                 "path": "telecom.where(system = %phone_system and use = %home_use).value.first()",
                 "type": "string"},
                {"name": "home_email",
                 "path": "telecom.where(system = %email_system and use = %home_use).value.first()",
                 "type": "string"},
                {"name": "any_phone",
                 "path": "telecom.where(system = %phone_system).value.first()",
                 "type": "string"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample Input:**

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

**Expected Output:**
| patient_id | home_phone | home_email | any_phone |
|------------|------------------|--------------------|------------------|
| patient-1 | +1-617-555-1234 | john.doe@email.com | +1-617-555-1234 |

**Key Features:**

- Constants (prefixed with `%`) make expressions more readable and maintainable
- Constants can be reused across multiple columns
- Demonstrates complex filtering with multiple criteria
- Shows how to extract specific telecom entries by system and use

---

### Union operations

#### Combined patient and contact addresses

This example demonstrates the `unionAll` feature to combine data from different
sources into a single view.

**View Definition (JSON):**

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
            ],
            "unionAll": [
                {
                    "column": [
                        {
                            "name": "address_type",
                            "path": "'patient'",
                            "type": "string"
                        },
                        {
                            "name": "use",
                            "path": "use",
                            "type": "code"
                        },
                        {
                            "name": "street",
                            "path": "line.join('\\n')",
                            "type": "string"
                        },
                        {
                            "name": "city",
                            "path": "city",
                            "type": "string"
                        },
                        {
                            "name": "postal_code",
                            "path": "postalCode",
                            "type": "string"
                        },
                        {
                            "name": "contact_relationship",
                            "path": "''",
                            "type": "string"
                        }
                    ],
                    "forEach": "address"
                },
                {
                    "column": [
                        {
                            "name": "address_type",
                            "path": "'contact'",
                            "type": "string"
                        },
                        {
                            "name": "use",
                            "path": "address.use.first()",
                            "type": "code"
                        },
                        {
                            "name": "street",
                            "path": "address.line.join('\\n').first()",
                            "type": "string"
                        },
                        {
                            "name": "city",
                            "path": "address.city.first()",
                            "type": "string"
                        },
                        {
                            "name": "postal_code",
                            "path": "address.postalCode.first()",
                            "type": "string"
                        },
                        {
                            "name": "contact_relationship",
                            "path": "relationship.coding.display.join(', ')",
                            "type": "string"
                        }
                    ],
                    "forEach": "contact.where(address.exists())"
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
                        ),
                        unionAll = list(
                                # Patient addresses
                                list(
                                        column = list(
                                                list(name = "address_type", path = "'patient'", type = "string"),
                                                list(name = "use", path = "use", type = "code"),
                                                list(name = "street", path = "line.join('\\n')", type = "string"),
                                                list(name = "city", path = "city", type = "string"),
                                                list(name = "postal_code", path = "postalCode", type = "string"),
                                                list(name = "contact_relationship", path = "''", type = "string")
                                        ),
                                        forEach = "address"
                                ),
                                # Contact addresses  
                                list(
                                        column = list(
                                                list(name = "address_type", path = "'contact'", type = "string"),
                                                list(name = "use", path = "address.use.first()", type = "code"),
                                                list(name = "street", path = "address.line.join('\\n').first()", type = "string"),
                                                list(name = "city", path = "address.city.first()", type = "string"),
                                                list(name = "postal_code", path = "address.postalCode.first()", type = "string"),
                                                list(name = "contact_relationship", path = "relationship.coding.display.join(', ')", type = "string")
                                        ),
                                        forEach = "contact.where(address.exists())"
                                )
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
            ],
            "unionAll": [
                # Patient addresses
                {
                    "column": [
                        {"name": "address_type", "path": "'patient'",
                         "type": "string"},
                        {"name": "use", "path": "use", "type": "code"},
                        {"name": "street", "path": "line.join('\\n')",
                         "type": "string"},
                        {"name": "city", "path": "city", "type": "string"},
                        {"name": "postal_code", "path": "postalCode",
                         "type": "string"},
                        {"name": "contact_relationship", "path": "''",
                         "type": "string"}
                    ],
                    "forEach": "address"
                },
                # Contact addresses  
                {
                    "column": [
                        {"name": "address_type", "path": "'contact'",
                         "type": "string"},
                        {"name": "use", "path": "address.use.first()",
                         "type": "code"},
                        {"name": "street",
                         "path": "address.line.join('\\n').first()",
                         "type": "string"},
                        {"name": "city", "path": "address.city.first()",
                         "type": "string"},
                        {"name": "postal_code",
                         "path": "address.postalCode.first()",
                         "type": "string"},
                        {"name": "contact_relationship",
                         "path": "relationship.coding.display.join(', ')",
                         "type": "string"}
                    ],
                    "forEach": "contact.where(address.exists())"
                }
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Sample Input:**

```json
{
    "resourceType": "Patient",
    "id": "patient-1",
    "address": [
        {
            "use": "home",
            "line": [
                "123 Patient St"
            ],
            "city": "Boston",
            "postalCode": "02101"
        }
    ],
    "contact": [
        {
            "relationship": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0131",
                            "code": "E",
                            "display": "Emergency Contact"
                        }
                    ]
                }
            ],
            "address": {
                "use": "home",
                "line": [
                    "456 Contact Ave"
                ],
                "city": "Cambridge",
                "postalCode": "02139"
            }
        }
    ]
}
```

**Expected Output:**
| patient_id | address_type | use | street | city | postal_code |
contact_relationship |
|------------|--------------|------|-----------------|-----------|-------------|---------------------|
| patient-1 | patient | home | 123 Patient St | Boston | 02101 | |
| patient-1 | contact | home | 456 Contact Ave | Cambridge | 02139 | Emergency
Contact |

**Key Features:**

- `unionAll` combines rows from different data sources with the same schema
- Uses literal strings (like `'patient'`) to tag the source of each row
- Shows different `forEach` expressions for each union part
- Demonstrates handling optional data with empty strings and existence checks

---

### US Core blood pressure observations

This example shows how to flatten complex observations with components,
specifically blood pressure readings.

**View Definition (JSON):**

```json
{
    "resource": "Observation",
    "status": "active",
    "constant": [
        {
            "name": "systolic_bp_code",
            "valueCode": "8480-6"
        },
        {
            "name": "diastolic_bp_code",
            "valueCode": "8462-4"
        },
        {
            "name": "bp_panel_code",
            "valueCode": "85354-9"
        },
        {
            "name": "loinc_system",
            "valueUri": "http://loinc.org"
        }
    ],
    "where": [
        {
            "path": "code.coding.where(system = %loinc_system and code = %bp_panel_code).exists()"
        }
    ],
    "select": [
        {
            "column": [
                {
                    "name": "observation_id",
                    "path": "getResourceKey()"
                },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id",
                    "description": "Reference to the patient this observation is for"
                },
                {
                    "name": "encounter_id",
                    "path": "encounter.getReferenceKey(Encounter)",
                    "type": "id"
                },
                {
                    "name": "effective_datetime",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime"
                },
                {
                    "name": "issued_datetime",
                    "path": "issued",
                    "type": "instant"
                }
            ]
        },
        {
            "column": [
                {
                    "name": "systolic_value",
                    "path": "value.ofType(Quantity).value",
                    "type": "decimal"
                },
                {
                    "name": "systolic_unit",
                    "path": "value.ofType(Quantity).unit",
                    "type": "string"
                },
                {
                    "name": "systolic_system",
                    "path": "value.ofType(Quantity).system",
                    "type": "uri"
                },
                {
                    "name": "systolic_code",
                    "path": "value.ofType(Quantity).code",
                    "type": "code"
                }
            ],
            "forEach": "component.where(code.coding.where(system = %loinc_system and code = %systolic_bp_code).exists()).first()"
        },
        {
            "column": [
                {
                    "name": "diastolic_value",
                    "path": "value.ofType(Quantity).value",
                    "type": "decimal"
                },
                {
                    "name": "diastolic_unit",
                    "path": "value.ofType(Quantity).unit",
                    "type": "string"
                },
                {
                    "name": "diastolic_system",
                    "path": "value.ofType(Quantity).system",
                    "type": "uri"
                },
                {
                    "name": "diastolic_code",
                    "path": "value.ofType(Quantity).code",
                    "type": "code"
                }
            ],
            "forEach": "component.where(code.coding.where(system = %loinc_system and code = %diastolic_bp_code).exists()).first()"
        }
    ]
}
```

<Tabs>
<TabItem value="r" label="R">

```r
result <- data_source %>% ds_view(
        resource = "Observation",
        constants = list(
                list(name = "systolic_bp_code", valueCode = "8480-6"),
                list(name = "diastolic_bp_code", valueCode = "8462-4"),
                list(name = "bp_panel_code", valueCode = "85354-9"),
                list(name = "loinc_system", valueUri = "http://loinc.org")
        ),
        where = list(
                list(path = "code.coding.where(system = %loinc_system and code = %bp_panel_code).exists()")
        ),
        select = list(
                list(
                        column = list(
                                list(name = "observation_id", path = "getResourceKey()"),
                                list(name = "patient_id", path = "subject.getReferenceKey(Patient)", type = "id"),
                                list(name = "encounter_id", path = "encounter.getReferenceKey(Encounter)", type = "id"),
                                list(name = "effective_datetime", path = "effective.ofType(dateTime)", type = "dateTime"),
                                list(name = "issued_datetime", path = "issued", type = "instant")
                        )
                ),
                # Systolic component
                list(
                        column = list(
                                list(name = "systolic_value", path = "value.ofType(Quantity).value", type = "decimal"),
                                list(name = "systolic_unit", path = "value.ofType(Quantity).unit", type = "string"),
                                list(name = "systolic_system", path = "value.ofType(Quantity).system", type = "uri"),
                                list(name = "systolic_code", path = "value.ofType(Quantity).code", type = "code")
                        ),
                        forEach = "component.where(code.coding.where(system = %loinc_system and code = %systolic_bp_code).exists()).first()"
                ),
                # Diastolic component  
                list(
                        column = list(
                                list(name = "diastolic_value", path = "value.ofType(Quantity).value", type = "decimal"),
                                list(name = "diastolic_unit", path = "value.ofType(Quantity).unit", type = "string"),
                                list(name = "diastolic_system", path = "value.ofType(Quantity).system", type = "uri"),
                                list(name = "diastolic_code", path = "value.ofType(Quantity).code", type = "code")
                        ),
                        forEach = "component.where(code.coding.where(system = %loinc_system and code = %diastolic_bp_code).exists()).first()"
                )
        )
)
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = data_source.view(
    resource="Observation",
    constants=[
        {"name": "systolic_bp_code", "valueCode": "8480-6"},
        {"name": "diastolic_bp_code", "valueCode": "8462-4"},
        {"name": "bp_panel_code", "valueCode": "85354-9"},
        {"name": "loinc_system", "valueUri": "http://loinc.org"}
    ],
    where=[
        {
            "path": "code.coding.where(system = %loinc_system and code = %bp_panel_code).exists()"}
    ],
    select=[
        {
            "column": [
                {"name": "observation_id", "path": "getResourceKey()"},
                {"name": "patient_id",
                 "path": "subject.getReferenceKey(Patient)", "type": "id"},
                {"name": "encounter_id",
                 "path": "encounter.getReferenceKey(Encounter)", "type": "id"},
                {"name": "effective_datetime",
                 "path": "effective.ofType(dateTime)", "type": "dateTime"},
                {"name": "issued_datetime", "path": "issued", "type": "instant"}
            ]
        },
        # Systolic component
        {
            "column": [
                {"name": "systolic_value",
                 "path": "value.ofType(Quantity).value", "type": "decimal"},
                {"name": "systolic_unit", "path": "value.ofType(Quantity).unit",
                 "type": "string"},
                {"name": "systolic_system",
                 "path": "value.ofType(Quantity).system", "type": "uri"},
                {"name": "systolic_code", "path": "value.ofType(Quantity).code",
                 "type": "code"}
            ],
            "forEach": "component.where(code.coding.where(system = %loinc_system and code = %systolic_bp_code).exists()).first()"
        },
        # Diastolic component  
        {
            "column": [
                {"name": "diastolic_value",
                 "path": "value.ofType(Quantity).value", "type": "decimal"},
                {"name": "diastolic_unit",
                 "path": "value.ofType(Quantity).unit", "type": "string"},
                {"name": "diastolic_system",
                 "path": "value.ofType(Quantity).system", "type": "uri"},
                {"name": "diastolic_code",
                 "path": "value.ofType(Quantity).code", "type": "code"}
            ],
            "forEach": "component.where(code.coding.where(system = %loinc_system and code = %diastolic_bp_code).exists()).first()"
        }
    ]
)
```

</TabItem>
</Tabs>

**Expected Output:**
| observation_id | patient_id | effective_datetime | systolic_value |
systolic_unit | diastolic_value | diastolic_unit |
|---------------|------------|---------------------|----------------|---------------|-----------------|----------------|
| obs-bp-1 | patient-1 | 2023-10-15T10:30:00 | 120 | mmHg | 80 | mmHg |

**Key Features:**

- Filters observations to only include blood pressure panels using specific
  LOINC codes
- Uses `getReferenceKey()` to extract patient and encounter IDs from references
- Demonstrates `ofType()` to handle polymorphic fields (effective can be
  dateTime or Period)
- Shows how to extract values from observation components
- Uses constants for LOINC codes to improve maintainability

---

### Condition flattening with categories

This example shows how to flatten condition resources with their various coding
systems.

**View Definition (JSON):**

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
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id"
                },
                {
                    "name": "encounter_id",
                    "path": "encounter.getReferenceKey(Encounter)",
                    "type": "id"
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
            "column": [
                {
                    "name": "code_system",
                    "path": "system",
                    "type": "uri"
                },
                {
                    "name": "code",
                    "path": "code",
                    "type": "code"
                },
                {
                    "name": "display",
                    "path": "display",
                    "type": "string"
                }
            ],
            "forEachOrNull": "code.coding"
        },
        {
            "column": [
                {
                    "name": "category_system",
                    "path": "system",
                    "type": "uri"
                },
                {
                    "name": "category_code",
                    "path": "code",
                    "type": "code"
                },
                {
                    "name": "category_display",
                    "path": "display",
                    "type": "string"
                }
            ],
            "forEachOrNull": "category.coding"
        },
        {
            "column": [
                {
                    "name": "clinical_status",
                    "path": "code",
                    "type": "code"
                }
            ],
            "forEachOrNull": "clinicalStatus.coding"
        },
        {
            "column": [
                {
                    "name": "verification_status",
                    "path": "code",
                    "type": "code"
                }
            ],
            "forEachOrNull": "verificationStatus.coding"
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
                # Basic condition info
                list(
                        column = list(
                                list(name = "condition_id", path = "getResourceKey()"),
                                list(name = "patient_id", path = "subject.getReferenceKey(Patient)", type = "id"),
                                list(name = "encounter_id", path = "encounter.getReferenceKey(Encounter)", type = "id"),
                                list(name = "onset_datetime", path = "onset.ofType(dateTime)", type = "dateTime"),
                                list(name = "recorded_date", path = "recordedDate", type = "dateTime")
                        )
                ),
                # Condition codes
                list(
                        column = list(
                                list(name = "code_system", path = "system", type = "uri"),
                                list(name = "code", path = "code", type = "code"),
                                list(name = "display", path = "display", type = "string")
                        ),
                        forEachOrNull = "code.coding"
                ),
                # Categories
                list(
                        column = list(
                                list(name = "category_system", path = "system", type = "uri"),
                                list(name = "category_code", path = "code", type = "code"),
                                list(name = "category_display", path = "display", type = "string")
                        ),
                        forEachOrNull = "category.coding"
                ),
                # Clinical status
                list(
                        column = list(
                                list(name = "clinical_status", path = "code", type = "code")
                        ),
                        forEachOrNull = "clinicalStatus.coding"
                ),
                # Verification status
                list(
                        column = list(
                                list(name = "verification_status", path = "code", type = "code")
                        ),
                        forEachOrNull = "verificationStatus.coding"
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
        # Basic condition info
        {
            "column": [
                {"name": "condition_id", "path": "getResourceKey()"},
                {"name": "patient_id",
                 "path": "subject.getReferenceKey(Patient)", "type": "id"},
                {"name": "encounter_id",
                 "path": "encounter.getReferenceKey(Encounter)", "type": "id"},
                {"name": "onset_datetime", "path": "onset.ofType(dateTime)",
                 "type": "dateTime"},
                {"name": "recorded_date", "path": "recordedDate",
                 "type": "dateTime"}
            ]
        },
        # Condition codes
        {
            "column": [
                {"name": "code_system", "path": "system", "type": "uri"},
                {"name": "code", "path": "code", "type": "code"},
                {"name": "display", "path": "display", "type": "string"}
            ],
            "forEachOrNull": "code.coding"
        },
        # Categories
        {
            "column": [
                {"name": "category_system", "path": "system", "type": "uri"},
                {"name": "category_code", "path": "code", "type": "code"},
                {"name": "category_display", "path": "display",
                 "type": "string"}
            ],
            "forEachOrNull": "category.coding"
        },
        # Clinical status
        {
            "column": [
                {"name": "clinical_status", "path": "code", "type": "code"}
            ],
            "forEachOrNull": "clinicalStatus.coding"
        },
        # Verification status
        {
            "column": [
                {"name": "verification_status", "path": "code", "type": "code"}
            ],
            "forEachOrNull": "verificationStatus.coding"
        }
    ],
    constants=[
        {"name": "systolic_bp_code", "valueCode": "8480-6"},
        {"name": "diastolic_bp_code", "valueCode": "8462-4"},
        {"name": "bp_panel_code", "valueCode": "85354-9"},
        {"name": "loinc_system", "valueUri": "http://loinc.org"}
    ],
    where=[
        {
            "path": "code.coding.where(system = %loinc_system and code = %bp_panel_code).exists()"}
    ]
)
```

</TabItem>
</Tabs>

**Key Features:**

- Uses `forEachOrNull` to ensure rows are created even when coding arrays are
  empty
- Demonstrates handling multiple coding systems for the same condition
- Shows extraction of status codings (clinical and verification status)
- Uses `ofType()` for polymorphic onset field (could be dateTime, Age, Period,
  etc.)

---

### Working with extensions

#### US Core patient extensions

This example demonstrates extracting data from FHIR extensions, specifically US
Core patient extensions.

**View Definition (JSON):**

```json
{
    "resource": "Patient",
    "status": "active",
    "constant": [
        {
            "name": "race_extension_url",
            "valueUri": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
        },
        {
            "name": "ethnicity_extension_url",
            "valueUri": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
        },
        {
            "name": "birthsex_extension_url",
            "valueUri": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex"
        },
        {
            "name": "omb_category_url",
            "valueString": "ombCategory"
        },
        {
            "name": "text_url",
            "valueString": "text"
        }
    ],
    "select": [
        {
            "column": [
                {
                    "name": "patient_id",
                    "path": "getResourceKey()"
                },
                {
                    "name": "gender",
                    "path": "gender",
                    "type": "code"
                },
                {
                    "name": "birth_date",
                    "path": "birthDate",
                    "type": "date"
                },
                {
                    "name": "birth_sex",
                    "path": "extension.where(url = %birthsex_extension_url).value.ofType(code)",
                    "type": "code",
                    "description": "US Core birth sex extension"
                },
                {
                    "name": "race_omb_category",
                    "path": "extension.where(url = %race_extension_url).extension.where(url = %omb_category_url).value.ofType(Coding).code.first()",
                    "type": "code",
                    "description": "Primary race category from OMB"
                },
                {
                    "name": "race_text",
                    "path": "extension.where(url = %race_extension_url).extension.where(url = %text_url).value.ofType(string)",
                    "type": "string",
                    "description": "Race as free text"
                },
                {
                    "name": "ethnicity_omb_category",
                    "path": "extension.where(url = %ethnicity_extension_url).extension.where(url = %omb_category_url).value.ofType(Coding).code.first()",
                    "type": "code"
                },
                {
                    "name": "ethnicity_text",
                    "path": "extension.where(url = %ethnicity_extension_url).extension.where(url = %text_url).value.ofType(string)",
                    "type": "string"
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
        constants = list(
                list(name = "race_extension_url",
                     valueUri = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"),
                list(name = "ethnicity_extension_url",
                     valueUri = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"),
                list(name = "birthsex_extension_url",
                     valueUri = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex"),
                list(name = "omb_category_url", valueString = "ombCategory"),
                list(name = "text_url", valueString = "text")
        ),
        select = list(
                list(
                        column = list(
                                list(name = "patient_id", path = "getResourceKey()"),
                                list(name = "gender", path = "gender", type = "code"),
                                list(name = "birth_date", path = "birthDate", type = "date"),
                                list(name = "birth_sex",
                                     path = "extension.where(url = %birthsex_extension_url).value.ofType(code)",
                                     type = "code"),
                                list(name = "race_omb_category",
                                     path = "extension.where(url = %race_extension_url).extension.where(url = %omb_category_url).value.ofType(Coding).code.first()",
                                     type = "code"),
                                list(name = "race_text",
                                     path = "extension.where(url = %race_extension_url).extension.where(url = %text_url).value.ofType(string)",
                                     type = "string"),
                                list(name = "ethnicity_omb_category",
                                     path = "extension.where(url = %ethnicity_extension_url).extension.where(url = %omb_category_url).value.ofType(Coding).code.first()",
                                     type = "code"),
                                list(name = "ethnicity_text",
                                     path = "extension.where(url = %ethnicity_extension_url).extension.where(url = %text_url).value.ofType(string)",
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
    constants=[
        {"name": "race_extension_url",
         "valueUri": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"},
        {"name": "ethnicity_extension_url",
         "valueUri": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"},
        {"name": "birthsex_extension_url",
         "valueUri": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex"},
        {"name": "omb_category_url", "valueString": "ombCategory"},
        {"name": "text_url", "valueString": "text"}
    ],
    select=[
        {
            "column": [
                {"name": "patient_id", "path": "getResourceKey()"},
                {"name": "gender", "path": "gender", "type": "code"},
                {"name": "birth_date", "path": "birthDate", "type": "date"},
                {"name": "birth_sex",
                 "path": "extension.where(url = %birthsex_extension_url).value.ofType(code)",
                 "type": "code"},
                {"name": "race_omb_category",
                 "path": "extension.where(url = %race_extension_url).extension.where(url = %omb_category_url).value.ofType(Coding).code.first()",
                 "type": "code"},
                {"name": "race_text",
                 "path": "extension.where(url = %race_extension_url).extension.where(url = %text_url).value.ofType(string)",
                 "type": "string"},
                {"name": "ethnicity_omb_category",
                 "path": "extension.where(url = %ethnicity_extension_url).extension.where(url = %omb_category_url).value.ofType(Coding).code.first()",
                 "type": "code"},
                {"name": "ethnicity_text",
                 "path": "extension.where(url = %ethnicity_extension_url).extension.where(url = %text_url).value.ofType(string)",
                 "type": "string"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Key Features:**

- Shows how to extract values from simple extensions (birth sex)
- Demonstrates navigation of complex nested extensions (race, ethnicity)
- Uses `ofType()` to handle polymorphic extension values
- Constants make complex extension URLs manageable
- Shows extracting both coded values and text from the same extension

---

### Cross-resource references and keys

#### Patient-encounter summary

This example shows how to create a view that can be easily joined with other
views using reference keys.

**View Definition (JSON):**

```json
{
    "resource": "Encounter",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "encounter_id",
                    "path": "getResourceKey()",
                    "type": "id"
                },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id",
                    "description": "Can be joined with patient views using this key"
                },
                {
                    "name": "encounter_class_system",
                    "path": "class.system",
                    "type": "uri"
                },
                {
                    "name": "encounter_class_code",
                    "path": "class.code",
                    "type": "code"
                },
                {
                    "name": "encounter_status",
                    "path": "status",
                    "type": "code"
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
                },
                {
                    "name": "length_minutes",
                    "path": "length.value",
                    "type": "decimal"
                },
                {
                    "name": "service_provider_id",
                    "path": "serviceProvider.getReferenceKey(Organization)",
                    "type": "id"
                }
            ]
        },
        {
            "column": [
                {
                    "name": "type_system",
                    "path": "system",
                    "type": "uri"
                },
                {
                    "name": "type_code",
                    "path": "code",
                    "type": "code"
                },
                {
                    "name": "type_display",
                    "path": "display",
                    "type": "string"
                }
            ],
            "forEachOrNull": "type.coding"
        },
        {
            "column": [
                {
                    "name": "diagnosis_condition_id",
                    "path": "condition.getReferenceKey(Condition)",
                    "type": "id"
                },
                {
                    "name": "diagnosis_use_system",
                    "path": "use.system",
                    "type": "uri"
                },
                {
                    "name": "diagnosis_use_code",
                    "path": "use.code",
                    "type": "code"
                },
                {
                    "name": "diagnosis_rank",
                    "path": "rank",
                    "type": "positiveInt"
                }
            ],
            "forEach": "diagnosis"
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
                # Basic encounter info  
                list(
                        column = list(
                                list(name = "encounter_id", path = "getResourceKey()", type = "id"),
                                list(name = "patient_id", path = "subject.getReferenceKey(Patient)", type = "id"),
                                list(name = "encounter_class_system", path = "class.system", type = "uri"),
                                list(name = "encounter_class_code", path = "class.code", type = "code"),
                                list(name = "encounter_status", path = "status", type = "code"),
                                list(name = "period_start", path = "period.start", type = "dateTime"),
                                list(name = "period_end", path = "period.end", type = "dateTime"),
                                list(name = "length_minutes", path = "length.value", type = "decimal"),
                                list(name = "service_provider_id", path = "serviceProvider.getReferenceKey(Organization)", type = "id")
                        )
                ),
                # Encounter types
                list(
                        column = list(
                                list(name = "type_system", path = "system", type = "uri"),
                                list(name = "type_code", path = "code", type = "code"),
                                list(name = "type_display", path = "display", type = "string")
                        ),
                        forEachOrNull = "type.coding"
                ),
                # Diagnoses
                list(
                        column = list(
                                list(name = "diagnosis_condition_id", path = "condition.getReferenceKey(Condition)", type = "id"),
                                list(name = "diagnosis_use_system", path = "use.system", type = "uri"),
                                list(name = "diagnosis_use_code", path = "use.code", type = "code"),
                                list(name = "diagnosis_rank", path = "rank", type = "positiveInt")
                        ),
                        forEach = "diagnosis"
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
        # Basic encounter info  
        {
            "column": [
                {"name": "encounter_id", "path": "getResourceKey()",
                 "type": "id"},
                {"name": "patient_id",
                 "path": "subject.getReferenceKey(Patient)", "type": "id"},
                {"name": "encounter_class_system", "path": "class.system",
                 "type": "uri"},
                {"name": "encounter_class_code", "path": "class.code",
                 "type": "code"},
                {"name": "encounter_status", "path": "status", "type": "code"},
                {"name": "period_start", "path": "period.start",
                 "type": "dateTime"},
                {"name": "period_end", "path": "period.end",
                 "type": "dateTime"},
                {"name": "length_minutes", "path": "length.value",
                 "type": "decimal"},
                {"name": "service_provider_id",
                 "path": "serviceProvider.getReferenceKey(Organization)",
                 "type": "id"}
            ]
        },
        # Encounter types
        {
            "column": [
                {"name": "type_system", "path": "system", "type": "uri"},
                {"name": "type_code", "path": "code", "type": "code"},
                {"name": "type_display", "path": "display", "type": "string"}
            ],
            "forEachOrNull": "type.coding"
        },
        # Diagnoses
        {
            "column": [
                {"name": "diagnosis_condition_id",
                 "path": "condition.getReferenceKey(Condition)", "type": "id"},
                {"name": "diagnosis_use_system", "path": "use.system",
                 "type": "uri"},
                {"name": "diagnosis_use_code", "path": "use.code",
                 "type": "code"},
                {"name": "diagnosis_rank", "path": "rank",
                 "type": "positiveInt"}
            ],
            "forEach": "diagnosis"
        }
    ]
)
```

</TabItem>
</Tabs>

**Key Features:**

- Uses `getReferenceKey()` to extract IDs that can be used for joins
- Shows how to handle encounter class (Coding datatype)
- Demonstrates extracting period start/end dates
- Uses `forEach` for diagnoses to create multiple rows per encounter
- Shows extraction of reference IDs from various reference fields

---

### Conditional logic and complex expressions

#### Patient risk stratification

This example shows advanced FHIRPath expressions with conditional logic.

**View Definition (JSON):**

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
                },
                {
                    "name": "age_years",
                    "path": "(today() - birthDate).days() / 365.25",
                    "type": "decimal"
                },
                {
                    "name": "age_category",
                    "path": "iif((today() - birthDate).days() / 365.25 < 18, 'pediatric', iif((today() - birthDate).days() / 365.25 < 65, 'adult', 'geriatric'))",
                    "type": "string",
                    "description": "Age-based risk category"
                },
                {
                    "name": "has_phone",
                    "path": "telecom.where(system = 'phone').exists()",
                    "type": "boolean"
                },
                {
                    "name": "has_email",
                    "path": "telecom.where(system = 'email').exists()",
                    "type": "boolean"
                },
                {
                    "name": "contact_score",
                    "path": "telecom.where(system = 'phone').count() + telecom.where(system = 'email').count()",
                    "type": "integer",
                    "description": "Simple contact method availability score"
                },
                {
                    "name": "primary_language",
                    "path": "communication.where(preferred = true).language.coding.where(system = 'urn:ietf:bcp:47').code.first()",
                    "type": "code"
                },
                {
                    "name": "deceased_flag",
                    "path": "deceased.exists()",
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
                                list(name = "patient_id", path = "getResourceKey()"),
                                list(name = "age_years", path = "(today() - birthDate).days() / 365.25", type = "decimal"),
                                list(name = "age_category",
                                     path = "iif((today() - birthDate).days() / 365.25 < 18, 'pediatric', iif((today() - birthDate).days() / 365.25 < 65, 'adult', 'geriatric'))",
                                     type = "string"),
                                list(name = "has_phone", path = "telecom.where(system = 'phone').exists()", type = "boolean"),
                                list(name = "has_email", path = "telecom.where(system = 'email').exists()", type = "boolean"),
                                list(name = "contact_score",
                                     path = "telecom.where(system = 'phone').count() + telecom.where(system = 'email').count()",
                                     type = "integer"),
                                list(name = "primary_language",
                                     path = "communication.where(preferred = true).language.coding.where(system = 'urn:ietf:bcp:47').code.first()",
                                     type = "code"),
                                list(name = "deceased_flag", path = "deceased.exists()", type = "boolean")
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
                {"name": "patient_id", "path": "getResourceKey()"},
                {"name": "age_years",
                 "path": "(today() - birthDate).days() / 365.25",
                 "type": "decimal"},
                {"name": "age_category",
                 "path": "iif((today() - birthDate).days() / 365.25 < 18, 'pediatric', iif((today() - birthDate).days() / 365.25 < 65, 'adult', 'geriatric'))",
                 "type": "string"},
                {"name": "has_phone",
                 "path": "telecom.where(system = 'phone').exists()",
                 "type": "boolean"},
                {"name": "has_email",
                 "path": "telecom.where(system = 'email').exists()",
                 "type": "boolean"},
                {"name": "contact_score",
                 "path": "telecom.where(system = 'phone').count() + telecom.where(system = 'email').count()",
                 "type": "integer"},
                {"name": "primary_language",
                 "path": "communication.where(preferred = true).language.coding.where(system = 'urn:ietf:bcp:47').code.first()",
                 "type": "code"},
                {"name": "deceased_flag", "path": "deceased.exists()",
                 "type": "boolean"}
            ]
        }
    ]
)
```

</TabItem>
</Tabs>

**Key Features:**

- Uses `iif()` (immediate if) for conditional logic
- Demonstrates date arithmetic for age calculation
- Shows counting and aggregation within expressions
- Uses `exists()` function for boolean flags
- Complex navigation through communication preferences

---

## Best Practices and Tips

### Performance Considerations

- Use `where` clauses to filter early and reduce processing
- Consider using `forEachOrNull` instead of `forEach` when you need consistent
  row counts
- Use constants for frequently referenced values like code systems

### Maintainability

- Use descriptive column names and include descriptions for complex expressions
- Group related constants together
- Comment complex FHIRPath expressions in your documentation

### Data Quality

- Handle missing data explicitly with null checks
- Use `ofType()` when dealing with polymorphic fields
- Consider using `first()` or other collection functions to handle unexpected
  multiples

### Common Patterns

- `getResourceKey()` for consistent resource identification
- `getReferenceKey(ResourceType)` for extracting reference IDs
- `.coding.where(system = 'url').code` for extracting specific codes
- `.join(' ')` for combining arrays into strings
- `.exists()` for boolean flags

## Error Handling

If your view definition contains errors, Pathling will provide detailed error
messages. Common issues include:

1. **Invalid FHIRPath expressions**: Check syntax and function usage
2. **Type mismatches**: Ensure column types match the data being extracted
3. **Missing constants**: Ensure all referenced constants are defined
4. **Invalid resource types**: Check that the resource type exists in your FHIR
   version

## Further Reading

- [SQL on FHIR Specification](https://sql-on-fhir.org/)
- [FHIRPath Specification](http://hl7.org/fhirpath/)
- [Pathling Documentation](https://pathling.csiro.au/docs)
- [US Core Implementation Guide](http://hl7.org/fhir/us/core/)
