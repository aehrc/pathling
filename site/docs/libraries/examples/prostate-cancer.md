---
sidebar_position: 1
title: Prostate cancer risk factors
description: Example of running SQL on FHIR queries using the Pathling libraries.
---

# Prostate cancer risk factors

This example demonstrates how to use SQL on FHIR views to query a set of risk
factors from FHIR data for the purpose of identifying patients at a high-risk of
being diagnosed with prostate cancer.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext
from pyspark.sql.functions import to_date, to_timestamp, round, min, nth_value
from pyspark.sql.window import Window
```

</TabItem>
<TabItem value="r" label="R">

```r
library(pathling)
library(sparklyr)
library(dplyr)
```

</TabItem>
</Tabs>

As a first step, we will initialise the Pathling context.

You can call this with no arguments and it will either create a new Spark
session with some sensible defaults, or pick up an existing Spark session.

<Tabs>
<TabItem value="python" label="Python">

```python
pc = PathlingContext.create()
pc.spark.sql("CREATE SCHEMA IF NOT EXISTS paper_lg")
pc.spark.catalog.setCurrentDatabase("paper_lg")
```

</TabItem>
<TabItem value="r" label="R">

```r
pc <- pathling_connect()
DBI::dbExecute(pc, "CREATE SCHEMA IF NOT EXISTS paper_lg")
DBI::dbExecute(pc, "USE paper_lg")
```

</TabItem>
</Tabs>

# Read data from tables

Now we will read some data from a set of previously persisted Delta tables. The
data we are using here is
a [Synthea](https://synthetichealth.github.io/synthea/) dataset containing
approximately 10,000 patients.

The object returned is a "data source", which contains each of the data frames
that have been encoded, as well as methods to run queries over them.

<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.tables()
```

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_from_tables()
```

</TabItem>
</Tabs>

Here are the resources that are contained within our new data source:

<Tabs>
<TabItem value="python" label="Python">

```python
data.resource_types()
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_resource_types()
```

</TabItem>
</Tabs>

```
    ['Condition',
     'Immunization',
     'MedicationRequest',
     'Organization',
     'CareTeam',
     'Device',
     'DiagnosticReport',
     'ImagingStudy',
     'Patient',
     'ExplanationOfBenefit',
     'Practitioner',
     'AllergyIntolerance',
     'CarePlan',
     'SupplyDelivery',
     'Observation',
     'Encounter',
     'MedicationAdministration',
     'Procedure',
     'Claim']
```

Here are the row counts for some of the resources:

<Tabs>
<TabItem value="python" label="Python">

```python
data.read('Patient').count()
```

```
    11515
```

```python
data.read('Observation').count()
```

```
    2881995
```

```python
data.read('Condition').count()
```

```
    373288
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_read('Patient') %>% sdf_nrow()
```

```
    11515
```

```r
data %>% ds_read('Observation') %>% sdf_nrow()
```

```
    2881995
```

```r
data %>% ds_read('Condition') %>% sdf_nrow()
```

```
    373288
```

</TabItem>
</Tabs>

# Prostate cancer risk factors

To demonstrate the SQL on FHIR query functionality, we will describe a simple
scenario that involves the extraction of data to support an analysis of prostate
cancer risk factors.

We will extract some patient demographic data along with some candidate risk
factors:

- Hyperlipidemia (recorded total cholesterol > 240 mg/dL)
- High BMI (recorded BMI > 30 kg/m2)

Our target view will be a table containing one row per patient, with the
following columns:

- Patient ID
- Birth date
- Postal code
- Deceased status
- Prostate cancer onset
- High BMI observed date
- Hyperlipidemia observed date
- Total cholesterol (mg/dL)

In order to construct this view, we will create four different views, and then
compose them together using regular Spark SQL:

- Patient demographics
- Hyperlipidemia observations
- High BMI observations
- Prostate cancer diagnoses

## Patient demographic view

This view will provide the following data elements:

- Patient ID
- Birth date
- Postal code
- Deceased status

Patient ID will be extracted using the `getResourceKey()` function. This returns
an identifier that can be used along with the `getReferenceKey()` function to
join the related rows from different views together.

Birth date and deceased time are pretty straightforward, and are singular so do
not require any kind of unnesting.

Postal code returns multiple values, as a patient can have more than one
address. In this example we have chosen to keep this as an array, by setting the
`collection` element to `true`.

<Tabs>
<TabItem value="python" label="Python">

```python
patients = data.view(
    "Patient",
    select=[
        {
            "column": [
                {
                    "description": "Patient ID",
                    "path": "getResourceKey()",
                    "name": "id",
                },
                {
                    "description": "Birth date",
                    "path": "birthDate",
                    "name": "birth_date",
                },
                {
                    "description": "Postal code",
                    "path": "address.postalCode",
                    "name": "postal_code",
                    "collection": True
                },
                {
                    "description": "Deceased time",
                    "path": "deceased.ofType(dateTime)",
                    "name": "deceased",
                },
            ]
        },
    ],
)
```

</TabItem>
<TabItem value="r" label="R">

```r
patients <- data %>%
        ds_view(
                "Patient",
                select = list(
                        column = list(
                                list(
                                        description = "Patient ID",
                                        path = "getResourceKey()",
                                        name = "id"
                                ),
                                list(
                                        description = "Birth date",
                                        path = "birthDate",
                                        name = "birth_date"
                                ),
                                list(
                                        description = "Postal code",
                                        path = "address.postalCode",
                                        name = "postal_code",
                                        collection = TRUE
                                ),
                                list(
                                        description = "Deceased time",
                                        path = "deceased.ofType(dateTime)",
                                        name = "deceased"
                                )
                        )
                )
        )
```

</TabItem>
</Tabs>

<Tabs>
<TabItem value="python" label="Python">

```python
patients.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
patients %>% head(10) %>% collect()
```

</TabItem>
</Tabs>

| id                                           | birth_date | postal_code | deceased                  |
|----------------------------------------------|------------|-------------|---------------------------|
| Patient/801faac2-595a-3274-8a98-f6d78774310f | 1982-10-24 | [02128]     | NULL                      |
| Patient/801fb251-3ae3-8d3b-2519-89fb92592701 | 1931-03-28 | [02114]     | NULL                      |
| Patient/8024510b-8666-2688-08f1-6272e64ce607 | 2008-10-11 | [02780]     | NULL                      |
| Patient/80333a44-525d-5b2e-f86f-f49ba6ff22fb | 1957-07-27 | [02118]     | 1987-06-15T08:00:45+10:00 |
| Patient/80379569-fe92-82c5-5ca7-f525414f6adb | 1987-02-04 | [02148]     | NULL                      |
| Patient/803a5acc-45ab-66a4-e4d0-5ef7f035d4ab | 1982-04-15 | []          | NULL                      |
| Patient/803a8a7d-c4ab-c281-3660-2e998737187a | 1913-10-03 | [02132]     | NULL                      |
| Patient/8051dd33-ab42-a906-1b43-08e869a6c45e | 2002-08-07 | []          | NULL                      |
| Patient/80593f71-36be-a9da-7ada-4fc756caca86 | 2010-11-01 | [01604]     | NULL                      |
| Patient/8064f6c5-77d3-9652-10ca-332d2b80b078 | 1998-10-28 | [02118]     | NULL                      |

## Hyperlipidemia view

This view will provide the following data elements:

- Observation ID
- Patient ID
- Observation date
- Observation code
- Total cholesterol unit
- Total cholesterol value

The view will be filtered to only total cholesterol observations that exceed 240
mg/dL.

<Tabs>
<TabItem value="python" label="Python">

```python
cholesterol = data.view(
    "Observation",
    select=[
        {
            "column": [
                {
                    "description": "Observation ID",
                    "path": "getResourceKey()",
                    "name": "id",
                },
                {
                    "description": "Patient ID",
                    "path": "subject.getReferenceKey()",
                    "name": "patient_id",
                },
                {
                    "description": "Observation date",
                    "path": "effective.ofType(dateTime)",
                    "name": "date",
                },
            ],
            "select": [
                {
                    "forEach": "code.coding",
                    "column": [
                        {
                            "description": "Observation code",
                            "path": "code",
                            "name": "code",
                        },
                    ],
                },
                {
                    "forEach": "value.ofType(Quantity)",
                    "column": [
                        {
                            "description": "Total cholesterol unit",
                            "path": "unit",
                            "name": "unit",
                        },
                        {
                            "description": "Total cholesterol value",
                            "path": "value",
                            "name": "value",
                        },
                    ],
                },
            ],
        }
    ],
    where=[
        {
            "description": "Total cholesterol > 240 mg/dL",
            "path": "where(code.coding.exists(system = 'http://loinc.org'"
                    "and code = '2093-3'))"
                    ".value.ofType(Quantity).where(code = 'mg/dL')"
                    ".value > 240",
        }
    ],
)
```

</TabItem>
<TabItem value="r" label="R">

```r
cholesterol <- data %>%
        ds_view(
                "Observation",
                select = list(
                        column = list(
                                list(
                                        description = "Observation ID",
                                        path = "getResourceKey()",
                                        name = "id"
                                ),
                                list(
                                        description = "Patient ID",
                                        path = "subject.getReferenceKey()",
                                        name = "patient_id"
                                ),
                                list(
                                        description = "Observation date",
                                        path = "effective.ofType(dateTime)",
                                        name = "date"
                                )
                        ),
                        select = list(
                                list(
                                        forEach = "code.coding",
                                        column = list(
                                                list(
                                                        description = "Observation code",
                                                        path = "code",
                                                        name = "code"
                                                )
                                        )
                                ),
                                list(
                                        forEach = "value.ofType(Quantity)",
                                        column = list(
                                                list(
                                                        description = "Total cholesterol unit",
                                                        path = "unit",
                                                        name = "unit"
                                                ),
                                                list(
                                                        description = "Total cholesterol value",
                                                        path = "value",
                                                        name = "value"
                                                )
                                        )
                                )
                        )
                ),
                where = list(
                        list(
                                description = "Total cholesterol > 240 mg/dL",
                                path = paste0(
                                        "where(code.coding.exists(system = 'http://loinc.org'",
                                        "and code = '2093-3'))",
                                        ".value.ofType(Quantity).where(code = 'mg/dL')",
                                        ".value > 240"
                                )
                        )
                )
        )
```

</TabItem>
</Tabs>

<Tabs>
<TabItem value="python" label="Python">

```python
cholesterol.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
cholesterol %>% head(10) %>% collect()
```

</TabItem>
</Tabs>

| id                                               | patient_id                                   | date                      | code   | unit  | value  |
|--------------------------------------------------|----------------------------------------------|---------------------------|--------|-------|--------|
| Observation/a9d28e2b-fc1e-dc37-d2b8-54f2d7e1fce2 | Patient/68d99a0d-35f4-e8cb-35d0-eb54e3dd480a | 2018-12-16T19:09:44+10:00 | 2093-3 | mg/dL | 250.69 |
| Observation/a9deed27-9d92-88a8-b3fc-abd952492300 | Patient/9f9aa9cc-b79f-b05a-baf7-4ca3a6263b9e | 2012-12-13T18:16:12+10:00 | 2093-3 | mg/dL | 256.12 |
| Observation/aa177299-7073-063e-9bb3-ebd0c2d5aa5d | Patient/fa615cec-e629-3f65-b867-749101680d61 | 2020-01-18T16:42:38+10:00 | 2093-3 | mg/dL | 248.32 |
| Observation/aa454888-bc42-bafb-25db-cc33cad48fd8 | Patient/af0eb7c8-c910-2a57-50c3-ccc45cc36eaf | 2017-11-24T14:04:33+10:00 | 2093-3 | mg/dL | 272.96 |
| Observation/aac208e1-7989-915f-368b-c37a8ad9fe1b | Patient/bea6b76e-4d88-9a9f-2d79-dc15000e3f21 | 2015-02-21T03:44:57+10:00 | 2093-3 | mg/dL | 241.76 |
| Observation/abc50b62-8502-d66a-5a49-d3b50f21a68f | Patient/139e597e-1f4d-a261-7958-555b7f4ffd80 | 2012-06-21T02:25:09+10:00 | 2093-3 | mg/dL | 240.75 |
| Observation/abfcffcf-5aba-3b9a-f521-907f2f248eb4 | Patient/004d1fc0-8eb8-cc45-b3ea-4495c0b16d50 | 1972-11-15T09:17:20+10:00 | 2093-3 | mg/dL | 247.27 |
| Observation/ac009d08-fc3d-b5a0-57dc-45e7555e1e5d | Patient/ff7478da-d888-bc48-b636-d5233f8de938 | 2018-03-31T03:35:07+10:00 | 2093-3 | mg/dL | 251.84 |
| Observation/ac50aafa-7043-f900-e5a3-aa25d8311cf3 | Patient/b52d7e4f-632c-49cd-00f7-3b5b7d85f8ba | 2013-05-20T11:47:21+10:00 | 2093-3 | mg/dL | 291.25 |
| Observation/ac5efce4-ce67-eb23-338c-2e64961794c3 | Patient/b6dc56c4-91e9-2880-a8a0-5a438d7e19ff | 2012-08-02T08:17:10+10:00 | 2093-3 | mg/dL | 261.25 |

## BMI view

This view will provide the following data elements:

- Observation ID
- Patient ID
- Observation date
- Observation code
- BMI unit
- BMI value

The view will be filtered to only BMI observations that exceed 30 kg/m2.

One of the nice things about the Pathling implementation is that the encoding
process includes canonicalisation of UCUM units. This means that the comparison
of total cholesterol values in this query will also pick up observations that
were made with different but comparable units, such as mg/dL.

<Tabs>
<TabItem value="python" label="Python">

```python
bmi = data.view(
    "Observation",
    select=[
        {
            "column": [
                {
                    "description": "Observation ID",
                    "path": "getResourceKey()",
                    "name": "id",
                },
                {
                    "description": "Patient ID",
                    "path": "subject.getReferenceKey()",
                    "name": "patient_id",
                },
                {
                    "description": "Observation date",
                    "path": "effective.ofType(dateTime)",
                    "name": "date",
                },
            ],
            "select": [
                {
                    "forEach": "code.coding",
                    "column": [
                        {
                            "description": "Observation code",
                            "path": "code",
                            "name": "code",
                        },
                    ],
                },
                {
                    "forEach": "value.ofType(Quantity)",
                    "column": [
                        {
                            "description": "BMI unit",
                            "path": "unit",
                            "name": "unit",
                        },
                        {
                            "description": "BMI value",
                            "path": "value",
                            "name": "value",
                        },
                    ],
                },
            ],
        }
    ],
    where=[
        {
            "description": "BMI > 30 kg/m2",
            "path": "where(code.coding.exists(system = 'http://loinc.org'"
                    "and code = '39156-5'))"
                    ".value.ofType(Quantity).where(code = 'kg/m2')"
                    ".value > 30",
        }
    ],
)
```

</TabItem>
<TabItem value="r" label="R">

```r
bmi <- data %>%
        ds_view(
                "Observation",
                select = list(
                        column = list(
                                list(
                                        description = "Observation ID",
                                        path = "getResourceKey()",
                                        name = "id"
                                ),
                                list(
                                        description = "Patient ID",
                                        path = "subject.getReferenceKey()",
                                        name = "patient_id"
                                ),
                                list(
                                        description = "Observation date",
                                        path = "effective.ofType(dateTime)",
                                        name = "date"
                                )
                        ),
                        select = list(
                                list(
                                        forEach = "code.coding",
                                        column = list(
                                                list(
                                                        description = "Observation code",
                                                        path = "code",
                                                        name = "code"
                                                )
                                        )
                                ),
                                list(
                                        forEach = "value.ofType(Quantity)",
                                        column = list(
                                                list(
                                                        description = "BMI unit",
                                                        path = "unit",
                                                        name = "unit"
                                                ),
                                                list(
                                                        description = "BMI value",
                                                        path = "value",
                                                        name = "value"
                                                )
                                        )
                                )
                        )
                ),
                where = list(
                        list(
                                description = "BMI > 30 kg/m2",
                                path = paste0(
                                        "where(code.coding.exists(system = 'http://loinc.org'",
                                        "and code = '39156-5'))",
                                        ".value.ofType(Quantity).where(code = 'kg/m2')",
                                        ".value > 30"
                                )
                        )
                )
        )
```

</TabItem>
</Tabs>

<Tabs>
<TabItem value="python" label="Python">

```python
bmi.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
bmi %>% head(10) %>% collect()
```

</TabItem>
</Tabs>

| id                                               | patient_id                                   | date                      | code    | unit  | value |
|--------------------------------------------------|----------------------------------------------|---------------------------|---------|-------|-------|
| Observation/a96ce215-8a06-cd1d-639a-0e06fa771ee0 | Patient/5c1d48a9-e9c0-4b62-5384-ebe0fd0dc3bc | 2020-02-04T17:53:45+10:00 | 39156-5 | kg/m2 | 33.22 |
| Observation/a96d3264-4e89-ac70-ad85-32116cd06392 | Patient/e99277b9-a765-e168-8780-2dcf54a43efc | 2004-10-29T08:28:40+10:00 | 39156-5 | kg/m2 | 30.02 |
| Observation/a974a0b6-4914-0168-bf12-d852775d621c | Patient/a05ad089-2587-1e64-2c5f-6e855a8baa94 | 2015-04-09T22:24:40+10:00 | 39156-5 | kg/m2 | 30.09 |
| Observation/a9752afb-d88b-cd1d-7aa8-bd954a0f9539 | Patient/de01b661-e58b-aac1-ea38-35a91690d21c | 2012-12-13T12:04:10+10:00 | 39156-5 | kg/m2 | 31.01 |
| Observation/a978697c-6fff-c177-8a13-67b42f9846d1 | Patient/eab52e11-e0ee-fc45-9a00-9dee0f0f0447 | 2018-08-28T11:04:20+10:00 | 39156-5 | kg/m2 | 30.37 |
| Observation/a9793ea2-a7ff-5f49-d0ae-1fbf265f482f | Patient/2b206983-2d3a-2e07-77ac-5e49f9fdc4d3 | 2016-04-03T08:26:46+10:00 | 39156-5 | kg/m2 | 32.84 |
| Observation/a97bf446-27cc-235d-f886-209ca752aecd | Patient/d033196c-eaa6-5eaa-fd63-9cf3cd29ac7a | 2013-05-06T08:11:47+10:00 | 39156-5 | kg/m2 | 30.03 |
| Observation/a9803b33-61fe-f8e7-460c-7ff52493ef1a | Patient/47f13b5c-3785-8897-c19f-cfae4c545362 | 2019-10-17T20:23:26+10:00 | 39156-5 | kg/m2 | 30.34 |
| Observation/a986bd7f-f1b8-0291-5543-c43973c9450b | Patient/fec02692-95ba-f8f2-cca4-f0f47ecaa48f | 1972-10-07T11:50:23+10:00 | 39156-5 | kg/m2 | 30.1  |
| Observation/a988cd5b-2a54-2be7-94bd-614733fe08ab | Patient/615abef5-4c9d-a53d-e6b3-bc5c45cb2cb6 | 1994-01-06T01:48:39+10:00 | 39156-5 | kg/m2 | 30.41 |

## Prostate cancer diagnosis view

This view will provide the following data elements:

- Condition ID
- Patient ID
- Condition onset date
- Condition code

The view will be filtered to only prostate cancer diagnoses.

<Tabs>
<TabItem value="python" label="Python">

```python
prostate_cancer_diagnoses = data.view(
    "Condition",
    select=[
        {
            "column": [
                {
                    "description": "Condition ID",
                    "path": "getResourceKey()",
                    "name": "id",
                },
                {
                    "description": "Patient ID",
                    "path": "subject.getReferenceKey()",
                    "name": "patient_id",
                },
                {
                    "description": "SNOMED CT diagnosis code",
                    "path": "code.coding.where(system = 'http://snomed.info/sct').code",
                    "name": "sct_id",
                },
                {
                    "description": "Date of onset",
                    "path": "onsetDateTime",
                    "name": "onset",
                },
            ]
        }
    ],
    where=[
        {
            "description": "Neoplasm of prostate",
            "path": "code.coding.exists(system = 'http://snomed.info/sct'"
                    "and code = '126906006')",
        }
    ],
)
```

</TabItem>
<TabItem value="r" label="R">

```r
prostate_cancer_diagnoses <- data %>%
        ds_view(
                "Condition",
                select = list(
                        column = list(
                                list(
                                        description = "Condition ID",
                                        path = "getResourceKey()",
                                        name = "id"
                                ),
                                list(
                                        description = "Patient ID",
                                        path = "subject.getReferenceKey()",
                                        name = "patient_id"
                                ),
                                list(
                                        description = "SNOMED CT diagnosis code",
                                        path = "code.coding.where(system = 'http://snomed.info/sct').code",
                                        name = "sct_id"
                                ),
                                list(
                                        description = "Date of onset",
                                        path = "onsetDateTime",
                                        name = "onset"
                                )
                        )
                ),
                where = list(
                        list(
                                description = "Neoplasm of prostate",
                                path = paste0(
                                        "code.coding.exists(system = 'http://snomed.info/sct'",
                                        "and code = '126906006')"
                                )
                        )
                )
        )
```

</TabItem>
</Tabs>

<Tabs>
<TabItem value="python" label="Python">

```python
prostate_cancer_diagnoses.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
prostate_cancer_diagnoses %>%
        head(10) %>%
        collect()
```

</TabItem>
</Tabs>

| id                                             | patient_id                                   | sct_id    | onset                     |
|------------------------------------------------|----------------------------------------------|-----------|---------------------------|
| Condition/828b84ef-2de1-520e-9932-801c7ddfc307 | Patient/2e071829-78d6-6567-b5ce-c187c6cf8d9b | 126906006 | 2021-09-29T22:28:07+10:00 |
| Condition/840ab28b-70c2-92f1-38dc-7700416b82f9 | Patient/ea20b941-d402-2a76-7f7c-1adee8d14f2a | 126906006 | 1992-01-21T14:02:25+11:00 |
| Condition/847dfa4e-2b29-69b7-dbb4-9e5581480b6c | Patient/ee0db189-5938-08c2-880c-5c8cb1524540 | 126906006 | 2015-09-02T04:02:35+10:00 |
| Condition/85586193-fd10-0d27-aeed-e9cdeb8a7c8e | Patient/11a0b1b5-ce48-19a6-c20f-5f6da2749850 | 126906006 | 2003-11-22T07:15:17+10:00 |
| Condition/85949701-25ea-6ae0-51f5-fed415e3a472 | Patient/e2690804-78d4-0325-4e50-3f3940c3e220 | 126906006 | 1973-01-22T19:08:02+10:00 |
| Condition/8ae7698d-f130-2ec9-b116-8a316d9f1fa1 | Patient/e2dd0d99-4f66-65d1-f82e-2cc5a494b162 | 126906006 | 1987-06-13T18:53:06+10:00 |
| Condition/8b5961ce-acae-e0f6-8c1c-201f069e6aaa | Patient/4bac725c-f7de-7d84-317f-a91e89338578 | 126906006 | 1984-08-24T21:33:45+10:00 |
| Condition/8ba6382c-4c55-a174-de38-15d66b66007f | Patient/572840df-38ac-8303-02b5-320e850565bc | 126906006 | 1999-11-27T20:47:40+10:00 |
| Condition/8bc77d24-14f2-de4a-68e5-778dad91f30b | Patient/981164bb-bce7-9438-1bf4-ded5422f8200 | 126906006 | 1977-12-17T05:13:30+10:00 |
| Condition/8c1b6c7e-dd88-5042-4e50-816e8d30ff09 | Patient/ea68f9e2-f1a1-b920-e0c3-e2671d5ba987 | 126906006 | 1991-04-06T08:05:13+10:00 |

## Final view composition

Now that we have created the four subsidiary views, we can compose them together
to create our final view.

First we will create a new column on the prostate cancer diagnosis view to get
the first date of diagnosis (as there could be multiple).

<Tabs>
<TabItem value="python" label="Python">

```python
prostate_cancer_diagnoses = prostate_cancer_diagnoses.withColumn(
    "latest_onset",
    min(to_timestamp(prostate_cancer_diagnoses.onset)).over(
        Window.partitionBy(prostate_cancer_diagnoses.patient_id)
    ),
)
prostate_cancer_diagnoses.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
prostate_cancer_diagnoses <- prostate_cancer_diagnoses %>%
        group_by(patient_id) %>%
        mutate(latest_onset = min(as_datetime(onset))) %>%
        ungroup()

prostate_cancer_diagnoses %>%
        head(10) %>%
        collect()
```

</TabItem>
</Tabs>

| id                                             | patient_id                                   | sct_id    | onset                     | latest_onset        |
|------------------------------------------------|----------------------------------------------|-----------|---------------------------|---------------------|
| Condition/f9d41b9a-7044-304b-ad86-8fea1854711b | Patient/00c813fb-1a8c-2310-008e-e57dab9e0698 | 126906006 | 2000-12-07T03:43:57+10:00 | 2000-12-06 17:43:57 |
| Condition/16b1286f-e803-7941-f18e-000212f44e81 | Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 126906006 | 1982-02-28T08:41:54+10:00 | 1982-02-27 22:41:54 |
| Condition/a2b879de-2ddc-a9a7-d231-997e19fd6cf4 | Patient/0141f80a-700b-75a7-3f7f-91c788096fa8 | 126906006 | 1990-11-11T17:10:28+11:00 | 1990-11-11 06:10:28 |
| Condition/5efcf6ad-c1cd-de25-5500-3a8deac5ffa3 | Patient/01b4682f-47b4-b738-9ba1-e5f5506ae3ce | 126906006 | 2002-08-17T14:34:48+10:00 | 2002-08-17 04:34:48 |
| Condition/2397de38-8cd4-aab0-81f9-b7a06aa23918 | Patient/0203a0e5-99cf-6aa5-be69-b5adb47d709d | 126906006 | 1984-01-01T17:34:39+10:00 | 1984-01-01 07:34:39 |
| Condition/dd9fc0e5-57d2-58b4-9f0e-d20cd8838e45 | Patient/021f7437-04ed-40ce-9c49-5d940c8ae817 | 126906006 | 2006-06-04T08:03:25+10:00 | 2006-06-03 22:03:25 |
| Condition/6124f3ab-32aa-1560-82a8-72fb7af1691c | Patient/0417532a-96bf-2dcd-33c2-63829dc5f236 | 126906006 | 1976-02-23T10:18:23+10:00 | 1976-02-23 00:18:23 |
| Condition/0393fd53-1842-f9fc-e01a-ac63a5ce26be | Patient/06e0904b-8986-0c72-48bd-6313c723998a | 126906006 | 1975-07-20T16:07:42+10:00 | 1975-07-20 06:07:42 |
| Condition/53513614-78f9-51a1-f2cc-6cf49b86efe7 | Patient/07bf2136-666c-7236-e9d8-442fa423d22a | 126906006 | 2018-03-05T11:57:53+10:00 | 2018-03-05 01:57:53 |
| Condition/d5ec5877-3b01-702f-41d8-5eb37c9d6c48 | Patient/07cdcd2f-d848-f050-2022-a2b3184d5df6 | 126906006 | 2004-03-31T09:51:46+10:00 | 2004-03-30 23:51:46 |

Then we can join the diagnosis view to the patient demographics.

<Tabs>
<TabItem value="python" label="Python">

```python
patient_diagnoses = patients.join(
    prostate_cancer_diagnoses,
    patients.id == prostate_cancer_diagnoses.patient_id,
    "inner",
)
patient_diagnoses.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
patient_diagnoses <- patients %>%
        inner_join(
                prostate_cancer_diagnoses,
                by = c("id" = "patient_id")
        )

patient_diagnoses %>% head(10) %>% collect()
```

</TabItem>
</Tabs>

| id                                           | birth_date | postal_code | deceased                  | id                                             | patient_id                                   | sct_id    | onset                     | latest_onset        |
|----------------------------------------------|------------|-------------|---------------------------|------------------------------------------------|----------------------------------------------|-----------|---------------------------|---------------------|
| Patient/00c813fb-1a8c-2310-008e-e57dab9e0698 | 1925-07-22 | [02188]     | 2005-08-16T09:10:57+10:00 | Condition/f9d41b9a-7044-304b-ad86-8fea1854711b | Patient/00c813fb-1a8c-2310-008e-e57dab9e0698 | 126906006 | 2000-12-07T03:43:57+10:00 | 2000-12-06 17:43:57 |
| Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 1921-01-01 | [01027]     | 1985-05-17T13:13:54+10:00 | Condition/16b1286f-e803-7941-f18e-000212f44e81 | Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 126906006 | 1982-02-28T08:41:54+10:00 | 1982-02-27 22:41:54 |
| Patient/0141f80a-700b-75a7-3f7f-91c788096fa8 | 1921-07-30 | [01938]     | 1993-07-02T21:02:28+10:00 | Condition/a2b879de-2ddc-a9a7-d231-997e19fd6cf4 | Patient/0141f80a-700b-75a7-3f7f-91c788096fa8 | 126906006 | 1990-11-11T17:10:28+11:00 | 1990-11-11 06:10:28 |
| Patient/01b4682f-47b4-b738-9ba1-e5f5506ae3ce | 1930-04-12 | []          | 2007-01-05T02:11:00+10:00 | Condition/5efcf6ad-c1cd-de25-5500-3a8deac5ffa3 | Patient/01b4682f-47b4-b738-9ba1-e5f5506ae3ce | 126906006 | 2002-08-17T14:34:48+10:00 | 2002-08-17 04:34:48 |
| Patient/0203a0e5-99cf-6aa5-be69-b5adb47d709d | 1913-01-12 | []          | NULL                      | Condition/2397de38-8cd4-aab0-81f9-b7a06aa23918 | Patient/0203a0e5-99cf-6aa5-be69-b5adb47d709d | 126906006 | 1984-01-01T17:34:39+10:00 | 1984-01-01 07:34:39 |
| Patient/021f7437-04ed-40ce-9c49-5d940c8ae817 | 1941-03-15 | [01752]     | 2020-03-15T23:31:34+10:00 | Condition/dd9fc0e5-57d2-58b4-9f0e-d20cd8838e45 | Patient/021f7437-04ed-40ce-9c49-5d940c8ae817 | 126906006 | 2006-06-04T08:03:25+10:00 | 2006-06-03 22:03:25 |
| Patient/0417532a-96bf-2dcd-33c2-63829dc5f236 | 1915-04-26 | [01938]     | NULL                      | Condition/6124f3ab-32aa-1560-82a8-72fb7af1691c | Patient/0417532a-96bf-2dcd-33c2-63829dc5f236 | 126906006 | 1976-02-23T10:18:23+10:00 | 1976-02-23 00:18:23 |
| Patient/06e0904b-8986-0c72-48bd-6313c723998a | 1912-05-11 | []          | NULL                      | Condition/0393fd53-1842-f9fc-e01a-ac63a5ce26be | Patient/06e0904b-8986-0c72-48bd-6313c723998a | 126906006 | 1975-07-20T16:07:42+10:00 | 1975-07-20 06:07:42 |
| Patient/07bf2136-666c-7236-e9d8-442fa423d22a | 1940-10-06 | []          | 2021-12-17T20:47:00+10:00 | Condition/53513614-78f9-51a1-f2cc-6cf49b86efe7 | Patient/07bf2136-666c-7236-e9d8-442fa423d22a | 126906006 | 2018-03-05T11:57:53+10:00 | 2018-03-05 01:57:53 |
| Patient/07cdcd2f-d848-f050-2022-a2b3184d5df6 | 1925-10-21 | [02536]     | NULL                      | Condition/d5ec5877-3b01-702f-41d8-5eb37c9d6c48 | Patient/07cdcd2f-d848-f050-2022-a2b3184d5df6 | 126906006 | 2004-03-31T09:51:46+10:00 | 2004-03-30 23:51:46 |

For the hyperlipidemia view, the first thing we will do is join it to the
patient details and diagnoses - but limiting the target rows to only those
observed before the date of diagnosis.

<Tabs>
<TabItem value="python" label="Python">

```python
cholesterol = cholesterol.withColumn("timestamp",
                                     to_timestamp(cholesterol.date))
with_cholesterol = (patient_diagnoses.join(
    cholesterol,
    (patient_diagnoses.patient_id == cholesterol.patient_id)
    & (patient_diagnoses.latest_onset > cholesterol.timestamp),
    "left_outer"
))
```

</TabItem>
<TabItem value="r" label="R">

```r
cholesterol <- cholesterol %>%
        mutate(timestamp = as_datetime(date))

with_cholesterol <- patient_diagnoses %>%
        left_join(
                cholesterol,
                by = c("patient_id" = "patient_id"),
                sql_on = "patient_diagnoses.latest_onset > cholesterol.timestamp"
        )
```

</TabItem>
</Tabs>

Then we will add new columns to the table to get the latest cholesterol
observation date and value for each patient (prior to the diagnosis).

<Tabs>
<TabItem value="python" label="Python">

```python
window = Window.partitionBy(patient_diagnoses.patient_id).orderBy(
    cholesterol.timestamp.desc())
with_cholesterol = with_cholesterol.withColumn(
    "latest_cholesterol_date",
    nth_value(cholesterol.timestamp, 1).over(window)
).withColumn(
    "latest_cholesterol_value",
    nth_value(cholesterol.value, 1).over(window)
).withColumn(
    "latest_cholesterol_unit",
    nth_value(cholesterol.unit, 1).over(window)
)
```

</TabItem>
<TabItem value="r" label="R">

```r
with_cholesterol <- with_cholesterol %>%
        group_by(patient_id) %>%
        arrange(desc(timestamp)) %>%
        mutate(
                latest_cholesterol_date = first(timestamp),
                latest_cholesterol_value = first(value),
                latest_cholesterol_unit = first(unit)
        ) %>%
        ungroup()
```

</TabItem>
</Tabs>

Then we will select just the columns we need so far, ready for joining to the
BMI view.

<Tabs>
<TabItem value="python" label="Python">

```python
with_cholesterol = with_cholesterol.select(
    patients.id.alias("patient_id"),
    patients.birth_date,
    patients.postal_code,
    patients.deceased,
    prostate_cancer_diagnoses.latest_onset.alias("prostate_cancer_onset"),
    with_cholesterol.latest_cholesterol_date,
    with_cholesterol.latest_cholesterol_value,
    with_cholesterol.latest_cholesterol_unit,
)
```

</TabItem>
<TabItem value="r" label="R">

```r
with_cholesterol <- with_cholesterol %>%
        select(
                patient_id = id,
                birth_date,
                postal_code,
                deceased,
                prostate_cancer_onset = latest_onset,
                latest_cholesterol_date,
                latest_cholesterol_value,
                latest_cholesterol_unit
        )
```

</TabItem>
</Tabs>

We'll do something similar with the BMI to the cholesterol - joining only to
prior observations.

<Tabs>
<TabItem value="python" label="Python">

```python
bmi = bmi.withColumn("timestamp", to_timestamp(bmi.date))
with_bmi = (with_cholesterol.join(
    bmi,
    (with_cholesterol.patient_id == bmi.patient_id)
    & (with_cholesterol.prostate_cancer_onset > bmi.timestamp),
    "left_outer"
))
```

</TabItem>
<TabItem value="r" label="R">

```r
bmi <- bmi %>%
        mutate(timestamp = as_datetime(date))

with_bmi <- with_cholesterol %>%
        left_join(
                bmi,
                by = c("patient_id" = "patient_id"),
                sql_on = "with_cholesterol.prostate_cancer_onset > bmi.timestamp"
        )
```

</TabItem>
</Tabs>

And then we'll add new columns to get the latest BMI observation date and value
for each patient (prior to the diagnosis).

<Tabs>
<TabItem value="python" label="Python">

```python
window = Window.partitionBy(with_cholesterol.patient_id).orderBy(
    bmi.timestamp.desc())
with_bmi = with_bmi.withColumn(
    "latest_bmi_date",
    nth_value(bmi.timestamp, 1).over(window)
).withColumn(
    "latest_bmi_value",
    nth_value(bmi.value, 1).over(window)
).withColumn(
    "latest_bmi_unit",
    nth_value(bmi.unit, 1).over(window)
)
```

</TabItem>
<TabItem value="r" label="R">

```r
with_bmi <- with_bmi %>%
        group_by(patient_id) %>%
        arrange(desc(timestamp)) %>%
        mutate(
                latest_bmi_date = first(timestamp),
                latest_bmi_value = first(value),
                latest_bmi_unit = first(unit)
        ) %>%
        ungroup()
```

</TabItem>
</Tabs>

The view can be finished off with a final selection of the desired columns.

<Tabs>
<TabItem value="python" label="Python">

```python
result = with_bmi.select(
    with_cholesterol.patient_id,
    to_date(with_bmi.birth_date).alias("birth_date"),
    with_bmi.postal_code,
    with_bmi.deceased,
    with_bmi.prostate_cancer_onset,
    with_bmi.latest_bmi_date.alias("high_bmi_observed"),
    round(with_bmi.latest_bmi_value, 2).alias("bmi_value"),
    with_bmi.latest_bmi_unit.alias("bmi_unit"),
    with_bmi.latest_cholesterol_date.alias("hyperlipidemia_observed"),
    with_bmi.latest_cholesterol_value.alias("total_cholesterol_value"),
    with_bmi.latest_cholesterol_unit.alias("total_cholesterol_unit"),
)
```

</TabItem>
<TabItem value="r" label="R">

```r
result <- with_bmi %>%
        select(
                patient_id,
                birth_date = as_date(birth_date),
                postal_code,
                deceased,
                prostate_cancer_onset,
                high_bmi_observed = latest_bmi_date,
                bmi_value = round(latest_bmi_value, 2),
                bmi_unit = latest_bmi_unit,
                hyperlipidemia_observed = latest_cholesterol_date,
                total_cholesterol_value = latest_cholesterol_value,
                total_cholesterol_unit = latest_cholesterol_unit
        )
```

</TabItem>
</Tabs>

<Tabs>
<TabItem value="python" label="Python">

```python
result.printSchema()
```

```
    root
     |-- patient_id: string (nullable = true)
     |-- birth_date: date (nullable = true)
     |-- postal_code: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- deceased: string (nullable = true)
     |-- prostate_cancer_onset: timestamp (nullable = true)
     |-- high_bmi_observed: timestamp (nullable = true)
     |-- bmi_value: double (nullable = true)
     |-- bmi_unit: string (nullable = true)
     |-- hyperlipidemia_observed: timestamp (nullable = true)
     |-- total_cholesterol_value: string (nullable = true)
     |-- total_cholesterol_unit: string (nullable = true)
```

```python
result.show(10, truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
result %>% glimpse()
```

```
Columns: 11
$ patient_id                <chr>
$ birth_date                <date>
$ postal_code               <list>
$ deceased                  <chr>
$ prostate_cancer_onset     <dttm>
$ high_bmi_observed         <dttm>
$ bmi_value                 <dbl>
$ bmi_unit                  <chr>
$ hyperlipidemia_observed   <dttm>
$ total_cholesterol_value   <chr>
$ total_cholesterol_unit    <chr>
```

```r
result %>% head(10) %>% collect()
```

</TabItem>
</Tabs>

| patient_id                                   | birth_date | postal_code | deceased                  | prostate_cancer_onset | high_bmi_observed   | bmi_value | bmi_unit | hyperlipidemia_observed | total_cholesterol_value | total_cholesterol_unit |
|----------------------------------------------|------------|-------------|---------------------------|-----------------------|---------------------|-----------|----------|-------------------------|-------------------------|------------------------|
| Patient/00c813fb-1a8c-2310-008e-e57dab9e0698 | 1925-07-22 | [02188]     | 2005-08-16T09:10:57+10:00 | 2000-12-06 17:43:57   | NULL                | NULL      | NULL     | 1996-11-19 17:05:24     | 245.63                  | mg/dL                  |
| Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 1921-01-01 | [01027]     | 1985-05-17T13:13:54+10:00 | 1982-02-27 22:41:54   | 1981-02-27 22:05:51 | 30.06     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 1921-01-01 | [01027]     | 1985-05-17T13:13:54+10:00 | 1982-02-27 22:41:54   | 1981-02-27 22:05:51 | 30.06     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 1921-01-01 | [01027]     | 1985-05-17T13:13:54+10:00 | 1982-02-27 22:41:54   | 1981-02-27 22:05:51 | 30.06     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 1921-01-01 | [01027]     | 1985-05-17T13:13:54+10:00 | 1982-02-27 22:41:54   | 1981-02-27 22:05:51 | 30.06     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/00fe87cb-6692-5eb6-0770-67fda7c7f1e8 | 1921-01-01 | [01027]     | 1985-05-17T13:13:54+10:00 | 1982-02-27 22:41:54   | 1981-02-27 22:05:51 | 30.06     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/0141f80a-700b-75a7-3f7f-91c788096fa8 | 1921-07-30 | [01938]     | 1993-07-02T21:02:28+10:00 | 1990-11-11 06:10:28   | 1987-10-31 05:30:55 | 31.35     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/0141f80a-700b-75a7-3f7f-91c788096fa8 | 1921-07-30 | [01938]     | 1993-07-02T21:02:28+10:00 | 1990-11-11 06:10:28   | 1987-10-31 05:30:55 | 31.35     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/0141f80a-700b-75a7-3f7f-91c788096fa8 | 1921-07-30 | [01938]     | 1993-07-02T21:02:28+10:00 | 1990-11-11 06:10:28   | 1987-10-31 05:30:55 | 31.35     | kg/m2    | NULL                    | NULL                    | NULL                   |
| Patient/0141f80a-700b-75a7-3f7f-91c788096fa8 | 1921-07-30 | [01938]     | 1993-07-02T21:02:28+10:00 | 1990-11-11 06:10:28   | 1987-10-31 05:30:55 | 31.35     | kg/m2    | NULL                    | NULL                    | NULL                   |
