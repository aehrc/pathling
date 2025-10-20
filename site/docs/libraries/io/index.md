---
sidebar_position: 2
description: The Pathling library can read and write data in a variety of different formats.
---

# Data in and out

The Pathling library provides convenience functions that help you to read FHIR
data in ahead of querying it. Functions are also provided to assist with the
persistence of FHIR data in various formats

## Reading FHIR data

There are several ways of reading FHIR data and making it available for query.

### FHIR Bulk Data API

You can load data directly from a FHIR server that implements
the [FHIR Bulk Data Access API](https://hl7.org/fhir/uv/bulkdata/). This allows
you to efficiently extract large amounts of data from a FHIR server for
analysis.

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
                        .withFhirEndpointUrl(
                                "https://bulk-data.smarthealthit.org/fhir")
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

The Bulk Data API source supports all features of
the [FHIR Bulk Data Access specification](https://hl7.org/fhir/uv/bulkdata/),
including:

- System, group and patient level exports
- Filtering by resource types and elements
- Time-based filtering
- Associated data inclusion
- SMART authentication (both symmetric and asymmetric)

### NDJSON

You can load all the [NDJSON](https://hl7.org/fhir/R4/nd-json.html) files from a
directory, assuming the following naming scheme:

`[resource type].ndjson` OR `[resource type].[tag].ndjson`

Pathling will detect the resource type from the file name, and convert it to a
Spark dataset using the corresponding resource encoder.

The tag can be any string, and is used to accommodate multiple different files
that contain the same resource type. For example, you might have one file called
`Observation.chart.ndjson` and another called `Observation.lab.ndjson`.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

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
                                                  r"Outputevents|Lab|Mix|VitalSigns|VitalSignsED)?$",
                                                  file_name))
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
        Set.of("Patient", "Condition", "Immunization"), FhirMimeTypes.FHIR_JSON) 
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
        .dataset("Patient", patientDataset)
        .dataset("Condition", conditionDataset)
        .dataset("Immunization", immunizationDataset);
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
data.write().

ndjson("/tmp/ndjson");
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
data.write().

parquet("/usr/share/warehouse/parquet");
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

You can use the `save_mode` parameter to control how data is written:
- `OVERWRITE` - Replaces all existing data
- `MERGE` - Merges new data with existing data based on resource ID

When using `MERGE` mode, you can optionally specify `delete_on_merge=True` to delete
resources that exist in the destination but not in the source. This is useful for
synchronising a subset of data or removing stale records.

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
data.write().

delta("/usr/share/warehouse/delta");
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

You can use the `save_mode` parameter to control how data is written:
- `OVERWRITE` - Replaces all existing data
- `MERGE` - Merges new data with existing data based on resource ID (requires `table_format="delta"`)

When using `MERGE` mode with Delta format tables, you can optionally specify
`delete_on_merge=True` to delete resources that exist in the destination but not
in the source. This is useful for synchronising a subset of data or removing stale records.

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
data.write().

tables("test");
```

</TabItem>
</Tabs>

