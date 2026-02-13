# ViewDefinition Examples

Comprehensive examples for common SQL on FHIR patterns.

## Basic Patient Demographics

Simple extraction of patient attributes:

```json
{
    "resourceType": "ViewDefinition",
    "name": "patient_demographics",
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "id", "path": "id", "type": "id" },
                { "name": "gender", "path": "gender", "type": "code" },
                { "name": "birth_date", "path": "birthDate", "type": "date" },
                {
                    "name": "family_name",
                    "path": "name.first().family",
                    "type": "string"
                },
                {
                    "name": "given_name",
                    "path": "name.first().given.first()",
                    "type": "string"
                }
            ]
        }
    ]
}
```

**Output**: One row per Patient with scalar demographics.

## Patient Addresses (forEach)

Unnest patient addresses into separate rows:

```json
{
    "resourceType": "ViewDefinition",
    "name": "patient_addresses",
    "resource": "Patient",
    "status": "active",
    "select": [
        { "column": [{ "name": "patient_id", "path": "id", "type": "id" }] },
        {
            "forEach": "address",
            "column": [
                { "name": "use", "path": "use", "type": "code" },
                { "name": "line", "path": "line.join(', ')", "type": "string" },
                { "name": "city", "path": "city", "type": "string" },
                { "name": "state", "path": "state", "type": "string" },
                {
                    "name": "postal_code",
                    "path": "postalCode",
                    "type": "string"
                },
                { "name": "country", "path": "country", "type": "string" }
            ]
        }
    ]
}
```

**Output**: One row per address. Patients without addresses produce no rows.

## Patient Addresses with forEachOrNull

Keep patients even if they have no addresses:

```json
{
    "resourceType": "ViewDefinition",
    "name": "patient_addresses_all",
    "resource": "Patient",
    "status": "active",
    "select": [
        { "column": [{ "name": "patient_id", "path": "id", "type": "id" }] },
        {
            "forEachOrNull": "address",
            "column": [
                { "name": "city", "path": "city", "type": "string" },
                { "name": "state", "path": "state", "type": "string" }
            ]
        }
    ]
}
```

**Output**: Patients without addresses get one row with null city/state.

## Condition Flat View

Flatten conditions with coded diagnoses:

```json
{
    "resourceType": "ViewDefinition",
    "name": "condition_flat",
    "resource": "Condition",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "id", "path": "id", "type": "id" },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id"
                },
                {
                    "name": "clinical_status",
                    "path": "clinicalStatus.coding.first().code",
                    "type": "code"
                },
                {
                    "name": "verification_status",
                    "path": "verificationStatus.coding.first().code",
                    "type": "code"
                },
                {
                    "name": "code_system",
                    "path": "code.coding.first().system",
                    "type": "uri"
                },
                {
                    "name": "code",
                    "path": "code.coding.first().code",
                    "type": "code"
                },
                {
                    "name": "code_display",
                    "path": "code.coding.first().display",
                    "type": "string"
                },
                {
                    "name": "onset_date",
                    "path": "onset.ofType(dateTime)",
                    "type": "dateTime"
                },
                {
                    "name": "recorded_date",
                    "path": "recordedDate",
                    "type": "dateTime"
                }
            ]
        }
    ]
}
```

## Observations with Filtering

Filter observations by LOINC code:

```json
{
    "resourceType": "ViewDefinition",
    "name": "blood_pressure",
    "resource": "Observation",
    "status": "active",
    "constant": [{ "name": "bp_code", "valueString": "85354-9" }],
    "where": [
        {
            "path": "code.coding.where(system = 'http://loinc.org' and code = %bp_code).exists()"
        }
    ],
    "select": [
        {
            "column": [
                { "name": "id", "path": "id", "type": "id" },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id"
                },
                {
                    "name": "effective_date",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime"
                },
                {
                    "name": "systolic",
                    "path": "component.where(code.coding.where(code = '8480-6').exists()).value.ofType(Quantity).value",
                    "type": "decimal"
                },
                {
                    "name": "diastolic",
                    "path": "component.where(code.coding.where(code = '8462-4').exists()).value.ofType(Quantity).value",
                    "type": "decimal"
                }
            ]
        }
    ]
}
```

## Encounter with Diagnoses (Nested forEach)

Cross-product of encounter and its diagnoses:

```json
{
    "resourceType": "ViewDefinition",
    "name": "encounter_diagnoses",
    "resource": "Encounter",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "encounter_id", "path": "id", "type": "id" },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id"
                },
                { "name": "status", "path": "status", "type": "code" },
                { "name": "class_code", "path": "class.code", "type": "code" }
            ]
        },
        {
            "forEach": "diagnosis",
            "column": [
                {
                    "name": "diagnosis_reference",
                    "path": "condition.reference",
                    "type": "string"
                },
                {
                    "name": "diagnosis_use",
                    "path": "use.coding.first().code",
                    "type": "code"
                },
                {
                    "name": "diagnosis_rank",
                    "path": "rank",
                    "type": "positiveInt"
                }
            ]
        }
    ]
}
```

## Union of Telecoms

Combine patient and contact telecoms:

```json
{
    "resourceType": "ViewDefinition",
    "name": "all_patient_telecoms",
    "resource": "Patient",
    "status": "active",
    "select": [
        { "column": [{ "name": "patient_id", "path": "id", "type": "id" }] },
        {
            "unionAll": [
                {
                    "forEach": "telecom",
                    "column": [
                        {
                            "name": "source",
                            "path": "'patient'",
                            "type": "string"
                        },
                        { "name": "system", "path": "system", "type": "code" },
                        { "name": "value", "path": "value", "type": "string" },
                        { "name": "use", "path": "use", "type": "code" }
                    ]
                },
                {
                    "forEach": "contact.telecom",
                    "column": [
                        {
                            "name": "source",
                            "path": "'contact'",
                            "type": "string"
                        },
                        { "name": "system", "path": "system", "type": "code" },
                        { "name": "value", "path": "value", "type": "string" },
                        { "name": "use", "path": "use", "type": "code" }
                    ]
                }
            ]
        }
    ]
}
```

## Extensions (US Core)

Extract US Core extensions:

```json
{
    "resourceType": "ViewDefinition",
    "name": "patient_us_core",
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "id", "path": "id", "type": "id" },
                {
                    "name": "birth_sex",
                    "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex').value.ofType(code)",
                    "type": "code"
                },
                {
                    "name": "race_code",
                    "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').extension('ombCategory').value.ofType(Coding).code.first()",
                    "type": "code"
                },
                {
                    "name": "race_display",
                    "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').extension('text').value.ofType(string)",
                    "type": "string"
                },
                {
                    "name": "ethnicity_code",
                    "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity').extension('ombCategory').value.ofType(Coding).code.first()",
                    "type": "code"
                }
            ]
        }
    ]
}
```

## Shareable ViewDefinition

Full example with all metadata:

```json
{
    "resourceType": "ViewDefinition",
    "url": "https://example.org/ViewDefinition/patient-summary",
    "name": "patient_summary",
    "title": "Patient Summary View",
    "status": "active",
    "experimental": false,
    "description": "Summary view of patient demographics for analytics",
    "resource": "Patient",
    "fhirVersion": ["4.0.1", "5.0.0"],
    "select": [
        {
            "column": [
                {
                    "name": "id",
                    "path": "id",
                    "type": "id",
                    "description": "Patient logical ID"
                },
                {
                    "name": "gender",
                    "path": "gender",
                    "type": "code",
                    "description": "Administrative gender"
                },
                {
                    "name": "birth_date",
                    "path": "birthDate",
                    "type": "date",
                    "description": "Date of birth"
                },
                {
                    "name": "is_deceased",
                    "path": "deceased.exists()",
                    "type": "boolean",
                    "description": "Whether patient is deceased"
                }
            ]
        }
    ]
}
```

## MedicationRequest View

Extract medication prescriptions:

```json
{
    "resourceType": "ViewDefinition",
    "name": "medication_requests",
    "resource": "MedicationRequest",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "id", "path": "id", "type": "id" },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id"
                },
                { "name": "status", "path": "status", "type": "code" },
                { "name": "intent", "path": "intent", "type": "code" },
                {
                    "name": "medication_code",
                    "path": "medication.ofType(CodeableConcept).coding.first().code",
                    "type": "code"
                },
                {
                    "name": "medication_display",
                    "path": "medication.ofType(CodeableConcept).coding.first().display",
                    "type": "string"
                },
                {
                    "name": "authored_on",
                    "path": "authoredOn",
                    "type": "dateTime"
                },
                {
                    "name": "requester_id",
                    "path": "requester.getReferenceKey()",
                    "type": "id"
                }
            ]
        }
    ]
}
```

## DiagnosticReport with Results

Unnest diagnostic report results:

```json
{
    "resourceType": "ViewDefinition",
    "name": "diagnostic_report_results",
    "resource": "DiagnosticReport",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "report_id", "path": "id", "type": "id" },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id"
                },
                { "name": "status", "path": "status", "type": "code" },
                {
                    "name": "category",
                    "path": "category.first().coding.first().code",
                    "type": "code"
                },
                {
                    "name": "code",
                    "path": "code.coding.first().code",
                    "type": "code"
                },
                {
                    "name": "code_display",
                    "path": "code.coding.first().display",
                    "type": "string"
                },
                {
                    "name": "effective_date",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime"
                },
                { "name": "issued", "path": "issued", "type": "instant" }
            ]
        },
        {
            "forEach": "result",
            "column": [
                {
                    "name": "observation_reference",
                    "path": "reference",
                    "type": "string"
                }
            ]
        }
    ]
}
```

## QuestionnaireResponse Items (repeat)

Flatten all items from a QuestionnaireResponse regardless of nesting depth:

```json
{
    "resourceType": "ViewDefinition",
    "name": "questionnaire_response_items",
    "resource": "QuestionnaireResponse",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "response_id", "path": "id", "type": "id" },
                {
                    "name": "questionnaire",
                    "path": "questionnaire",
                    "type": "canonical"
                },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)",
                    "type": "id"
                },
                { "name": "authored", "path": "authored", "type": "dateTime" },
                { "name": "status", "path": "status", "type": "code" }
            ]
        },
        {
            "repeat": ["item", "answer.item"],
            "column": [
                { "name": "link_id", "path": "linkId", "type": "string" },
                { "name": "text", "path": "text", "type": "string" },
                {
                    "name": "answer_string",
                    "path": "answer.value.ofType(string).first()",
                    "type": "string"
                },
                {
                    "name": "answer_integer",
                    "path": "answer.value.ofType(integer).first()",
                    "type": "integer"
                },
                {
                    "name": "answer_boolean",
                    "path": "answer.value.ofType(boolean).first()",
                    "type": "boolean"
                },
                {
                    "name": "answer_date",
                    "path": "answer.value.ofType(date).first()",
                    "type": "date"
                },
                {
                    "name": "answer_coding_code",
                    "path": "answer.value.ofType(Coding).code.first()",
                    "type": "code"
                },
                {
                    "name": "answer_coding_display",
                    "path": "answer.value.ofType(Coding).display.first()",
                    "type": "string"
                }
            ]
        }
    ]
}
```

**Output**: One row per item at any nesting level. The `repeat` directive:

1. Starts at the QuestionnaireResponse root
2. Follows the `item` path to get top-level items
3. For each item, follows `answer.item` to get nested items within answers
4. Recursively continues until no more items exist
5. Unions all items from all levels into a flat table

This eliminates the need to know the maximum nesting depth in advance.

## Collection Columns

When arrays are acceptable in output (non-tabular):

```json
{
    "resourceType": "ViewDefinition",
    "name": "patient_all_names",
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                { "name": "id", "path": "id", "type": "id" },
                {
                    "name": "all_family_names",
                    "path": "name.family",
                    "type": "string",
                    "collection": true
                },
                {
                    "name": "all_given_names",
                    "path": "name.given",
                    "type": "string",
                    "collection": true
                }
            ]
        }
    ]
}
```

**Note**: Collection columns are not compatible with TabularViewDefinition profile.
