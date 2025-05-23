---
sidebar_position: 3
description: The Pathling library can be used to query datasets of FHIR resources using FHIRPath. This is useful for aggregating data, and creating custom views.
---

# FHIRPath query

The Pathling library can be used to query datasets of FHIR resources
using [FHIRPath](../fhirpath). This is useful for aggregating data, and creating
custom views.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## Extract

This operation allows a user to create arbitrary tabular extracts from FHIR
data, by specifying columns in terms of set of FHIRPath expressions that are
used to populate them. This is useful for preparing data for use within other
tools, and helps to alleviate some of the burden of dealing with FHIR data in
its raw form.

The query can also be optionally filtered by a set of FHIRPath expressions,
which are combined using Boolean AND logic.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, Expression as exp

pc = PathlingContext.create()
data = pc.read.ndjson("s3://somebucket/synthea/ndjson")

# For patients that have not received a COVID-19 vaccine, extract the given 
# name, family name, phone number and whether the patient has heart disease.
result = data.extract("Patient",
                      columns=[
                          exp("name.first().given.first()", "Given name"),
                          exp("name.first().family", "Family name"),
                          exp("telecom.where(system = 'phone').value",
                              "Phone number"),
                          exp("reverseResolve(Condition.subject).exists("
                              "code.subsumedBy(http://snomed.info/sct|56265001))",
                              "Heart disease")
                      ],
                      filters=[
                          "reverseResolve(Immunization.patient).vaccineCode"
                          ".exists(memberOf('https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines'))"
                          ".not()"]
                      )
display(result)
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data <- pc %>% pathling_read_ndjson("s3://somebucket/synthea/ndjson")

# For patients that have not received a COVID-19 vaccine, extract the given
# name, family name, phone number and whether the patient has heart disease.
result <- data %>%
        ds_extract(
                "Patient",
                columns = c(
                        "Given name" = "name.first().given.first()",
                        "Family name" = "name.first().family",
                        "Phone number" = "telecom.where(system = 'phone').value",
                        "Heart disease" = "reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|56265001))"
                ),
                filters = c("Heart disease" = "reverseResolve(Condition.subject).exists(code.subsumedBy(http://snomed.info/sct|56265001))")
        ) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.SparkSession
import org.hl7.fhir.r4.model.Enumerations.ResourceType

val pc = PathlingContext.create(spark, terminologyConfig)
val data = pc.read.ndjson("s3://somebucket/synthea/ndjson")

// For patients that have not received a COVID-19 vaccine, extract the given
// name, family name, phone number and whether the patient has heart disease.
val result = data.extract(ResourceType.PATIENT)
        .column("name.first().given.first()", "Given name")
        .column("name.first().family", "Family name")
        .column("telecom.where(system = 'phone').value", "Phone number")
        .column("reverseResolve(Condition.subject)" +
                ".exists(code.subsumedBy(http://snomed.info/sct|56265001))", "Heart disease")
        .filter("reverseResolve(Immunization.patient).vaccineCode" +
                ".memberOf('https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines').allFalse()")
        .execute()

display(result)
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import au.csiro.pathling.library.io.source.NdjsonSource;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        NdjsonSource data = pc.read.ndjson("s3://somebucket/synthea/ndjson");

        // For patients that have not received a COVID-19 vaccine, extract the given
        // name, family name, phone number and whether the patient has heart disease.
        Dataset<Row> result = data.extract(ResourceType.PATIENT)
                .column("name.first().given.first()", "Given name")
                .column("name.first().family", "Family name")
                .column("telecom.where(system = 'phone').value", "Phone number")
                .column("reverseResolve(Condition.subject)"
                                + ".exists(code.subsumedBy(http://snomed.info/sct|56265001))",
                        "Heart disease")
                .filter("reverseResolve(Immunization.patient).vaccineCode"
                        + ".memberOf('https://aehrc.csiro.au/fhir/ValueSet/covid-19-vaccines').allFalse()")
                .execute();

        result.show();
    }
}

```

</TabItem>
</Tabs>

The result of this query would look something like this:

| Given name | Family name | Phone number | Heart disease |
|------------|-------------|--------------|---------------|
| John       | Smith       | 0412345678   | false         |
| Jane       | Doe         | 0412345678   | true          |

## Aggregate

This operation allows a user to perform aggregate queries on FHIR data, by
specifying aggregation, grouping and filter expressions. Grouped results are
returned.

The aggregate operation is useful for exploratory data analysis, as well as
powering visualisations and other summarized views of the data.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, Expression as exp

pc = PathlingContext.create()
data = pc.read.ndjson("s3://somebucket/synthea/ndjson")

# Count the number of female patients, grouped by the type of diabetes that they 
# have been diagnosed with.
result = data.aggregate(
        "Patient",
        aggregations=[exp("count()", "Number of patients")],
        groupings=[
            exp("reverseResolve(Condition.subject)"
                ".where(code.subsumedBy(http://snomed.info/sct|73211009))" +
                ".code.coding.display()",
                "Type of diabetes")
        ],
        filters=["gender = 'female'"],
)

display(result)
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data <- pc %>% pathling_read_ndjson("s3://somebucket/synthea/ndjson")

# Count the number of female patients, grouped by the type of diabetes that they
# have been diagnosed with.
result <- data %>%
        ds_aggregate(
                "Patient",
                aggregations = c("Number of patients" = "count()"),
                groupings = c(
                        "Type of diabetes" = "reverseResolve(Condition.subject).where(code.subsumedBy(http://snomed.info/sct|73211009)).code.coding.display()"),
                filters = "gender = 'female'"
        ) %>% show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.SparkSession
import org.hl7.fhir.r4.model.Enumerations.ResourceType

val pc = PathlingContext.create()
val data = pc.read.ndjson("s3://somebucket/synthea/ndjson")

// Count the number of female patients, grouped by the type of diabetes that they
// have been diagnosed with.
val result = data.aggregate(ResourceType.PATIENT)
        .aggregation("count()", "Number of patients")
        .grouping("reverseResolve(Condition.subject)" +
                  ".where(code.subsumedBy(http://snomed.info/sct|73211009))" +
                  ".code.coding.display()",
            "Type of diabetes")
        .filter("gender = 'female'")
        .execute()

display(result)
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import au.csiro.pathling.library.io.source.NdjsonSource;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        NdjsonSource data = pc.read.ndjson("s3://somebucket/synthea/ndjson");

        // Count the number of female patients, grouped by the type of diabetes that they
        // have been diagnosed with.
        Dataset<Row> result = data.aggregate(ResourceType.PATIENT)
                .aggregation("count()", "Number of patients")
                .grouping("reverseResolve(Condition.subject)" +
                          ".where(code.subsumedBy(http://snomed.info/sct|73211009))" + 
                          ".code.coding.display()",
                        "Type of diabetes")
                .filter("gender = 'female'")
                .execute();

        result.show();
    }
}
```

</TabItem>
</Tabs>

The result of this query would look something like this:

| Type of diabetes                         | Number of patients |
|------------------------------------------|--------------------|
| Diabetes mellitus due to cystic fibrosis | 3                  |
| Type 2 diabetes mellitus                 | 122                |
| Type 1 diabetes mellitus                 | 14                 |
| NULL                                     | 1472               |

## Reading FHIR data

There are several ways of making FHIR data available for FHIRPath query.

### NDJSON

You can load all the [NDJSON](https://hl7.org/fhir/R4/nd-json.html) files from a
directory, assuming the following naming scheme:

`[resource type].ndjson` OR `[resource type].[tag].ndjson`

Pathling will detect the resource type from the file name, and convert it to a
Spark dataset using the corresponding resource encoder.

The tag can be any string, and is used to accommodate multiple different files
that contain the same resource type. For example, you might have one file called
`Observation.chart.ndjson` and another called `Observation.lab.ndjson`.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.ndjson("/usr/share/staging/ndjson")
```

</TabItem>
<TabItem value='r' label='R'>

```r
data <- pc %>% pathling_read_ndjson("/usr/share/staging/ndjson")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
val data = pc.read().ndjson("/usr/share/staging/ndjson")
```

</TabItem>
<TabItem value="java" label="Java">

```java
NdjsonSource data = pc.read().ndjson("/usr/share/staging/ndjson");
```

</TabItem>
</Tabs>

You can also accommodate a custom naming scheme within the NDJSON files by using
the [file_name_mapper](https://pathling.csiro.au/docs/python/pathling.html#pathling.datasource.DataSources.ndjson)
argument. Here is an example of how to import the
[MIMIC-IV FHIR data set](https://physionet.org/content/mimic-iv-fhir/1.0/):

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.ndjson(
        "/usr/share/staging/ndjson",
        file_name_mapper=lambda file_name: re.findall(r"Mimic(\w+?)(?:ED|ICU|"
        r"Chartevents|Datetimeevents|Labevents|MicroOrg|MicroSusc|MicroTest|"
        r"Outputevents|Lab|Mix|VitalSigns|VitalSignsED)?$", file_name))
```

</TabItem>
</Tabs>

### FHIR Bundles

You can load data from a directory containing either JSON or
XML [FHIR Bundles](https://hl7.org/fhir/R4/bundle.html). The specified resource
types will be extracted from the Bundles and made available for query.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.bundles("/usr/share/staging/bundles",
                       resource_types=["Patient", "Condition", "Immunization"])
```

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_read_bundles("/usr/share/staging/bundles",
                                resource_types = c("Patient", "Condition", "Immunization"))
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
val data = pc.read().bundles("/usr/share/staging/bundles",
    Set("Patient", "Condition", "Immunization").asJava, FhirMimeTypes.FHIR_JSON)
```

</TabItem>
<TabItem value="java" label="Java">

```java
BundlesSource data = pc.read().bundles("/usr/share/staging/bundles",
        Set.of("Patient","Condition","Immunization"),FhirMimeTypes.FHIR_JSON) 
```

</TabItem>
</Tabs>

### Datasets

You can make data that is already held in Spark datasets available for query
using the `datasets` method. This method returns an object that can be populated
with pairs of resource type and dataset, using the `dataset` method.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.datasets({
    "Patient": patient_dataset,
    "Condition": condition_dataset,
    "Immunization": immunization_dataset,
})
```

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_read_datasets(list(
        Patient = patient_dataset,
        Condition = condition_dataset,
        Immunization = immunization_dataset
))
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
val data = pc.read().datasets()
        .dataset("Patient", patientDataset)
        .dataset("Condition", conditionDataset)
        .dataset("Immunization", immunizationDataset)
```

</TabItem>
<TabItem value="java" label="Java">

```java
DatasetSource data = pc.read().datasets()
        .dataset("Patient",patientDataset)
        .dataset("Condition",conditionDataset)
        .dataset("Immunization",immunizationDataset);
```

</TabItem>
</Tabs>

### Parquet

You can load data from a directory
containing [Parquet](https://parquet.apache.org/) files. The Parquet files must
have been saved using the schema used by the Pathling encoders (
see [Writing FHIR data](#parquet-1)).

The files are assumed to be named according to their resource
type (`[resource type].parquet`), e.g. `Patient.parquet`, `Condition.parquet`.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.parquet("/usr/share/staging/parquet")
```

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_read_parquet("/usr/share/staging/parquet")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
val data = pc.read().parquet("/usr/share/staging/parquet")
```

</TabItem>
<TabItem value="java" label="Java">

```java
ParquetSource data = pc.read().parquet("/usr/share/staging/parquet");
```

</TabItem>
</Tabs>

### Delta Lake

You can load data from a directory containing [Delta Lake](https://delta.io/)
tables. Delta tables are a specialisation of Parquet that enable additional
functionality, such as incremental update and history. The Delta tables must
have been saved using the schema used by the Pathling encoders 
(see [Writing FHIR data](#delta-lake-1)).

Note that you will need to use
the [enable_delta](https://pathling.csiro.au/docs/python/pathling.html#pathling.context.PathlingContext)
parameter when initialising the Pathling context.

The files are assumed to be named according to their resource
type (`[resource type].parquet`), e.g. `Patient.parquet`, `Condition.parquet`.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.delta("/usr/share/staging/delta")
```

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_read_delta("/usr/share/staging/delta")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
val data = pc.read().delta("/usr/share/staging/delta")
```

</TabItem>
<TabItem value="java" label="Java">

```java
DeltaSource data = pc.read().delta("/usr/share/staging/delta");
```

</TabItem>
</Tabs>

### Managed tables

You can load data from managed tables that have previously been saved within
the [Spark catalog](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html).
You can optionally specify a schema that will be used to locate the tables,
otherwise the default schema will be used.

The tables are assumed to be named according to their resource
type, e.g. `Patient`, `Condition`.

This also works with
the [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
feature of Databricks.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.tables("mimic-iv")
```

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_read_tables("mimic-iv")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
val data = pc.read().tables("mimic-iv")
```

</TabItem>
<TabItem value="java" label="Java">

```java
CatalogSource data = pc.read().tables("mimic-iv");
```

</TabItem>
</Tabs>

### FHIR Bulk Data API

You can load data directly from a FHIR server that implements the [FHIR Bulk Data Access API](https://hl7.org/fhir/uv/bulkdata/). This allows you to efficiently extract large amounts of data from a FHIR server for analysis.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
# Basic system-level export
data = pc.read.bulk(
    fhir_endpoint_url="https://bulk-data.smarthealthit.org/fhir",
    output_dir="/tmp/bulk_export"
)

# Customized group-level export
data = pc.read.bulk(
    fhir_endpoint_url="https://bulk-data.smarthealthit.org/fhir",
    output_dir="/tmp/bulk_export",
    group_id="BMCHealthNet",
    types=["Patient", "Condition", "Observation"],
    elements=["id", "status"],
    since=datetime(2015, 1, 1, tzinfo=timezone.utc)
)

# Patient-level export with specific patients
data = pc.read.bulk(
    fhir_endpoint_url="https://bulk-data.smarthealthit.org/fhir",
    output_dir="/tmp/bulk_export",
    patients=[
        "Patient/736a19c8-eea5-32c5-67ad-1947661de21a",
        "Patient/26d06b50-7868-829d-cf71-9f9a68901a81"
    ]
)

# Export with authentication
data = pc.read.bulk(
    fhir_endpoint_url="https://bulk-data.smarthealthit.org/fhir",
    output_dir="/tmp/bulk_export",
    auth_config={
        "enabled": True,
        "client_id": "my-client-id",
        "private_key_jwk": "{ \"kty\":\"RSA\", ...}",
        "scope": "system/*.read"
    }
)
```

</TabItem>
<TabItem value="r" label="R">

```r
# Basic system-level export
data <- pc %>% pathling_read_bulk(
    fhir_endpoint_url = "https://bulk-data.smarthealthit.org/fhir",
    output_dir = "/tmp/bulk_export"
)

# Customized group-level export
data <- pc %>% pathling_read_bulk(
    fhir_endpoint_url = "https://bulk-data.smarthealthit.org/fhir",
    output_dir = "/tmp/bulk_export",
    group_id = "BMCHealthNet",
    types = c("Patient", "Condition", "Observation"),
    elements = c("id", "status"),
    since = as.POSIXct("2015-01-01", tz = "UTC")
)

# Patient-level export with specific patients
data <- pc %>% pathling_read_bulk(
    fhir_endpoint_url = "https://bulk-data.smarthealthit.org/fhir",
    output_dir = "/tmp/bulk_export",
    patients = c(
        "Patient/736a19c8-eea5-32c5-67ad-1947661de21a",
        "Patient/26d06b50-7868-829d-cf71-9f9a68901a81"
    )
)

# Export with authentication
data <- pc %>% pathling_read_bulk(
    fhir_endpoint_url = "https://bulk-data.smarthealthit.org/fhir",
    output_dir = "/tmp/bulk_export",
    auth_config = list(
        enabled = TRUE,
        client_id = "my-client-id",
        private_key_jwk = '{ "kty":"RSA", ...}',
        scope = "system/*.read"
    )
)
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
// Basic system-level export
val data = pc.read().bulk(
  BulkExportClient.systemBuilder()
    .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
    .withOutputDir("/tmp/bulk_export")
    .build()
)

// Customized group-level export
val data = pc.read().bulk(
  BulkExportClient.groupBuilder("BMCHealthNet")
    .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
    .withOutputDir("/tmp/bulk_export")
    .withTypes(List("Patient", "Condition", "Observation"))
    .withElements(List("id", "status"))
    .withSince(Instant.parse("2015-01-01T00:00:00Z"))
    .build()
)

// Patient-level export with specific patients
val data = pc.read().bulk(
  BulkExportClient.patientBuilder()
    .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
    .withOutputDir("/tmp/bulk_export")
    .withPatients(List(
      "Patient/736a19c8-eea5-32c5-67ad-1947661de21a",
      "Patient/26d06b50-7868-829d-cf71-9f9a68901a81"
    ))
    .build()
)

// Export with authentication
val authConfig = AuthConfig.builder()
  .enabled(true)
  .clientId("my-client-id")
  .privateKeyJWK("{ \"kty\":\"RSA\", ...}")
  .scope("system/*.read")
  .build()

val data = pc.read().bulk(
  BulkExportClient.systemBuilder()
    .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
    .withOutputDir("/tmp/bulk_export")
    .withAuthConfig(authConfig)
    .build()
)
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Basic system-level export
DataSource data = pc.read().bulk(
    BulkExportClient.systemBuilder()
        .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
        .withOutputDir("/tmp/bulk_export")
        .build()
);

// Customized group-level export
DataSource data = pc.read().bulk(
    BulkExportClient.groupBuilder("BMCHealthNet")
        .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
        .withOutputDir("/tmp/bulk_export")
        .withTypes(List.of("Patient", "Condition", "Observation"))
        .withElements(List.of("id", "status"))
        .withSince(Instant.parse("2015-01-01T00:00:00Z"))
        .build()
);

// Patient-level export with specific patients
DataSource data = pc.read().bulk(
    BulkExportClient.patientBuilder()
        .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
        .withOutputDir("/tmp/bulk_export")
        .withPatients(List.of(
            "Patient/736a19c8-eea5-32c5-67ad-1947661de21a",
            "Patient/26d06b50-7868-829d-cf71-9f9a68901a81"
        ))
        .build()
);

// Export with authentication
AuthConfig authConfig = AuthConfig.builder()
    .enabled(true)
    .clientId("my-client-id")
    .privateKeyJWK("{ \"kty\":\"RSA\", ...}")
    .scope("system/*.read")
    .build();

DataSource data = pc.read().bulk(
    BulkExportClient.systemBuilder()
        .withFhirEndpointUrl("https://bulk-data.smarthealthit.org/fhir")
        .withOutputDir("/tmp/bulk_export")
        .withAuthConfig(authConfig)
        .build()
);
```

</TabItem>
</Tabs>

The Bulk Data API source supports all features of the [FHIR Bulk Data Access specification](https://hl7.org/fhir/uv/bulkdata/), including:

- System, group and patient level exports
- Filtering by resource types and elements  
- Time-based filtering
- Associated data inclusion
- SMART authentication (both symmetric and asymmetric)

## Writing FHIR data

Once you have read data in from a data source, you can also optionally write it
back out to a variety of targets. This is useful for persisting source data in a
more efficient form for query (e.g. Parquet or Delta), or for exporting data to
NDJSON for use in other systems.

### NDJSON

You can write data to a directory containing NDJSON files. The files are named
according to their resource type (`[resource type].ndjson`), e.g.
`Patient.ndjson`, `Condition.ndjson`.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data.write.ndjson("/tmp/ndjson")
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_write_ndjson("/tmp/ndjson")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
data.write().ndjson("/tmp/ndjson")
```

</TabItem>
<TabItem value="java" label="Java">

```java
data.write().ndjson("/tmp/ndjson");
```

</TabItem>
</Tabs>

### Parquet

You can write data to a directory
containing [Parquet](https://parquet.apache.org/) files. The files are named
according to their resource type (`[resource type].parquet`),
e.g. `Patient.parquet`, `Condition.parquet`.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data.write.parquet("/usr/share/warehouse/parquet")
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_write_parquet("/usr/share/warehouse/parquet")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
data.write().parquet("/usr/share/warehouse/parquet")
```

</TabItem>
<TabItem value="java" label="Java">

```java
data.write().parquet("/usr/share/warehouse/parquet");
```

</TabItem>
</Tabs>

### Delta Lake

You can write data to a directory containing [Delta Lake](https://delta.io/)
tables. Delta tables are a specialisation of Parquet that enable additional
functionality, such as incremental update and history.

Note that you will need to use
the [enable_delta](https://pathling.csiro.au/docs/python/pathling.html#pathling.context.PathlingContext)
parameter when initialising the Pathling context.

The files are named according to their resource
type (`[resource type].parquet`), e.g. `Patient.parquet`, `Condition.parquet`.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data.write.delta("/usr/share/warehouse/delta")
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_write_delta("/usr/share/warehouse/delta")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
data.write().delta("/usr/share/warehouse/delta")
```

</TabItem>
<TabItem value="java" label="Java">

```java
data.write().delta("/usr/share/warehouse/delta");
```

</TabItem>
</Tabs>

### Managed tables

You can write data to managed tables that will be saved within
the [Spark catalog](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html).
You can optionally specify a schema that will be used to locate the tables,
otherwise the default schema will be used.

The tables are named according to their resource type,
e.g. `Patient`, `Condition`.

This also works with
the [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
feature of Databricks.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data.write.tables("test")
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_write_tables("test")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
data.write().tables("test")
```

</TabItem>
<TabItem value="java" label="Java">

```java
data.write().tables("test");
```

</TabItem>
</Tabs>

