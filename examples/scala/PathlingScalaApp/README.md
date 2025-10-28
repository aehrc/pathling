# Pathling Scala Example Application

This example demonstrates how to use the Pathling Java API within a Scala
project. The application shows how to query FHIR data using Pathling's SQL on
FHIR view runner within a Scala environment.

## Overview

The application demonstrates:

- Creating a Pathling context
- Reading NDJSON FHIR data
- Building and executing FHIR views to extract structured data

## Prerequisites

- Java 21
- Scala 2.12
- SBT (Scala Build Tool)
- NDJSON FHIR data files (for testing)

## Project Structure

```
PathlingScalaApp/
├── build.sbt                     # SBT build configuration
├── project/
│   ├── build.properties          # SBT version
│   └── plugins.sbt               # SBT plugins
└── src/main/scala/
    └── au/csiro/pathling/examples/
        └── PathlingScalaApp.scala # Main application
```

## Dependencies

The project uses the following key dependencies:

- `au.csiro.pathling:library-runtime:8.0.0-SNAPSHOT` - Pathling library for FHIR
  data processing
- `org.apache.spark:spark-sql:3.5.6` - Apache Spark 3.5.6, the same version used
  by Pathling 8

## Running the Application

1. Ensure you have NDJSON FHIR data available at `/tmp/ndjson`
2. From the project root directory, run:

```bash
sbt run
```

## Key Concepts

### PathlingContext

The `PathlingContext` is the entry point for all Pathling operations. It
provides methods to read FHIR data from various sources.

### FhirView

`FhirView` allows you to define structured queries against FHIR resources using
a declarative approach:

- `ofResource()` - Specifies the FHIR resource type to query
- `select()` - Defines the columns to extract
- `column()` - Extracts a single value using FHIRPath expressions
- `forEach()` - Iterates over collections (like codings in a CodeableConcept)

### FHIRPath Expressions

The example uses FHIRPath expressions like:

- `getResourceKey()` - Returns the primary identifier of the resource
- `code.coding` - Navigates to the coding elements within a code field
- `system`, `code`, `display` - Extracts specific properties from coding
  elements

## Data Requirements

The application expects NDJSON FHIR data containing Observation resources. Each
line should be a valid FHIR Observation resource in JSON format.

Example Observation:

```json
{
    "resourceType": "Observation",
    "id": "f9747cfe-8f03-8ff9-0b86-80cf5404ec27",
    "status": "final",
    "category": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "vital-signs",
                    "display": "vital-signs"
                }
            ]
        }
    ],
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "8302-2",
                "display": "Body Height"
            }
        ],
        "text": "Body Height"
    },
    "subject": {
        "reference": "Patient/b2ad1727-cc6f-c11f-2cad-e0430ddfb60b"
    },
    "encounter": {
        "reference": "Encounter/6db6fb6b-f7e3-56b3-0d0d-1e2fc64a6c25"
    },
    "effectiveDateTime": "2015-03-02T14:07:50+10:00",
    "issued": "2015-03-02T14:07:50.223+10:00",
    "valueQuantity": {
        "value": 54.7,
        "unit": "cm",
        "system": "http://unitsofmeasure.org",
        "code": "cm"
    }
}
```

## Example output

When you run the application with sample FHIR Observation data, you can expect
to see output similar to the following:

| patient_id                                       | code_system      | code_code | code_display                                                  |
|--------------------------------------------------|------------------|-----------|---------------------------------------------------------------|
| Observation/f9747cfe-8f03-8ff9-0b86-80cf5404ec27 | http://loinc.org | 8302-2    | Body Height                                                   |
| Observation/c00e77a5-cc35-4e12-c070-c681d9ca3ff8 | http://loinc.org | 72514-3   | Pain severity - 0-10 verbal numeric rating [Score] - Reported |
| Observation/333d87a4-ca3a-d3fb-dfae-3cfe407f40ec | http://loinc.org | 29463-7   | Body Weight                                                   |
| Observation/943837c3-5564-1139-7491-117f4cf9deb9 | http://loinc.org | 77606-2   | Weight-for-length Per age and sex                             |
| Observation/79537fc8-ac8a-2831-e657-5b9260238068 | http://loinc.org | 8302-2    | Body Height                                                   |
| Observation/1d77634a-c24a-05f2-2c20-e3b8d9ac1e6d | http://loinc.org | 8302-2    | Body Height                                                   |
| Observation/78b0fd9c-ba9b-4d12-e899-4c5bf312bb69 | http://loinc.org | 8302-2    | Body Height                                                   |
| Observation/7a0ae01d-549a-fa1e-0a19-fdb71b03fb5c | http://loinc.org | 9843-4    | Head Occipital-frontal circumference                          |
| Observation/23a86873-00ef-6000-282e-ecf9f14e61e4 | http://loinc.org | 72514-3   | Pain severity - 0-10 verbal numeric rating [Score] - Reported |
| Observation/e2a2eb53-10d5-90c6-9bf3-4d3d0d64bcbf | http://loinc.org | 72514-3   | Pain severity - 0-10 verbal numeric rating [Score] - Reported |
| Observation/d3d217d9-49b8-2bf5-863f-46f6b85713f9 | http://loinc.org | 29463-7   | Body Weight                                                   |
| Observation/7b0872b7-2983-2ad2-17b8-ed586f1cc7c2 | http://loinc.org | 29463-7   | Body Weight                                                   |
| Observation/318e1e37-1fa6-a04a-bf40-50031f55a13d | http://loinc.org | 85354-9   | Blood Pressure                                                |
| Observation/bbe5c4ee-2326-8dba-1635-13e749058a0e | http://loinc.org | 72514-3   | Pain severity - 0-10 verbal numeric rating [Score] - Reported |
| Observation/ea78a4a5-686b-e3a9-2663-df4a6f5dfe94 | http://loinc.org | 77606-2   | Weight-for-length Per age and sex                             |
| Observation/82c43a52-b268-cf74-efb5-c2df2d79f791 | http://loinc.org | 8867-4    | Heart rate                                                    |
| Observation/3ef730cf-8744-08c6-81b6-e7bf093e2a4e | http://loinc.org | 29463-7   | Body Weight                                                   |
| Observation/3f4c4d15-6bb1-d0bf-1b54-3c67f3aa4b23 | http://loinc.org | 9843-4    | Head Occipital-frontal circumference                          |
| Observation/eb170e05-fed2-326a-09cb-d79c4fe498ff | http://loinc.org | 9279-1    | Respiratory rate                                              |
| Observation/9960d02d-7666-f3f4-59ca-65222f4b73ea | http://loinc.org | 77606-2   | Weight-for-length Per age and sex                             |

We ran this on the `sm` dataset from
the [Pathling paper](https://doi.org/10.1186/s13326-022-00277-1), which is
available from
the [CSIRO Data Access Portal](https://doi.org/10.25919/er9r-0228).

## Customisation

To adapt this example for your use case:

1. **Change the data source**: Modify the `pc.read().ndjson()` call to point to
   your FHIR data location
2. **Select different resources**: Replace "Observation" with other FHIR
   resource types
3. **Extract different fields**: Modify the `column()` and `forEach()`
   expressions to extract the data you need
4. **Add filtering**: Use FHIRPath expressions to filter resources based on
   specific criteria

## Further Reading

- [Pathling documentation](https://pathling.csiro.au/docs)
- [FHIRPath specification](https://hl7.org/fhirpath)
- [FHIR specification](https://hl7.org/fhir)
