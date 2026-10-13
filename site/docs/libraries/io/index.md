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

You can load data from a directory containing
[Parquet](https://parquet.apache.org/) files written by Pathling (see
[Writing FHIR data](#parquet-1) and [Storage layout](./schema.md)).

The files are recognised as holding a resource type by their names, which start
with the resource type: `[resource type].parquet` or
`[resource type].[qualifier].parquet`, e.g. `Patient.parquet` or
`Patient.00000-7a60a041-0e7d-4e24-b23d-82405a665677.parquet`. All the files of a
resource type are read together as one dataset.

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
QueryableDataSource data = pc.read().parquet("/usr/share/staging/parquet");
```

</TabItem>
</Tabs>

#### Schema merging

Pathling stores each resource type with a schema that is
[fitted to the data](./schema.md#the-schema-is-fitted-to-the-data), so files
written at different times can carry different columns. The Parquet source
therefore merges the schemas of a resource type's files, and reads them as one
dataset whose schema is the union of theirs. Without merging, the schema of
whichever file happened to be read first would win, and the columns that only
other files carry would be silently lost.

Merging reads the footer of every file, which takes time on a large dataset. The
way to avoid that cost is to supply the schema to read a resource type with,
rather than to switch merging off, and there is deliberately no option to switch
it off. A supplied schema is used as given, so it must cover the columns of
every file of that type. Columns it leaves out are not read, and whole rows can
be lost as well as columns: a view that unnests a repeating element returns none
of a file's rows for that element if the schema leaves out every field the file
carries within it. The workable pattern is to merge once and keep the resulting
schema, or to take the schema of the data that was written, and supply that.
Resource types for which no schema is supplied are merged as usual.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
# Merge once, and keep the resulting schema.
patient_schema = pc.read.parquet("/usr/share/staging/parquet").read("Patient").schema

# Later reads use the schema rather than merging the schemas of the files.
data = pc.read.parquet(
    "/usr/share/staging/parquet", schemas={"Patient": patient_schema}
)
```

</TabItem>
<TabItem value="r" label="R">

```r
# Merge once, and keep the resulting schema as JSON.
patient_schema <- pc %>%
        pathling_read_parquet("/usr/share/staging/parquet") %>%
        ds_read("Patient") %>%
        sparklyr::spark_dataframe() %>%
        sparklyr::invoke("schema") %>%
        sparklyr::invoke("json")

# Later reads use the schema rather than merging the schemas of the files.
data <- pc %>% pathling_read_parquet("/usr/share/staging/parquet",
                                     schemas = list(Patient = patient_schema))
```

At most ten schemas can be supplied from R.

</TabItem>
<TabItem value="scala" label="Scala">

```scala
// Merge once, and keep the resulting schema.
val patientSchema = pc.read().parquet("/usr/share/staging/parquet")
        .read("Patient").schema

// Later reads use the schema rather than merging the schemas of the files.
val data = pc.read().parquetWithSchemas("/usr/share/staging/parquet",
    Map("Patient" -> patientSchema).asJava)
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Merge once, and keep the resulting schema.
StructType patientSchema = pc.read().parquet("/usr/share/staging/parquet")
        .read("Patient").schema();

// Later reads use the schema rather than merging the schemas of the files.
QueryableDataSource data = pc.read().parquetWithSchemas(
        "/usr/share/staging/parquet", Map.of("Patient", patientSchema));
```

</TabItem>
</Tabs>

### Delta Lake

You can load data from a directory containing [Delta Lake](https://delta.io/)
tables. Delta tables are a specialisation of Parquet that enable additional
functionality, such as incremental update and history. The Delta tables must
have been written by Pathling (see [Writing FHIR data](#delta-lake-1)). A Delta
table records its schema in its transaction log, so its schema is not merged
from its files.

Note that you will need to use
the [enable_delta](https://pathling.csiro.au/docs/python/pathling.html#pathling.context.PathlingContext)
parameter when initialising the Pathling context.

The tables are assumed to be named according to their resource
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
QueryableDataSource data = pc.read().delta("/usr/share/staging/delta");
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
QueryableDataSource data = pc.read().tables("mimic-iv");
```

</TabItem>
</Tabs>

### Layout detection

Earlier releases of Pathling stored resources in a
[different layout](./schema.md#data-written-by-earlier-releases), which this
release does not read. The Parquet, Delta and managed table sources check the
layout of each resource type as it is read, and refuse data in the earlier
layout with an error naming the resource type, the layout detected and the
remedy. Previously, such data was read and then failed later in a confusing way,
or gave wrong answers where the columns happened to line up.

Data in the earlier layout cannot be converted in place. Re-import it from its
source with this release, either into a new location or by overwriting the
existing one.

If data that was written by this release is misclassified by the check, the
check can be disabled for a single data source. Every other data source,
including others created from the same context, keeps checking. The option is
intended only for conforming data that is misclassified.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data = pc.read.parquet("/usr/share/staging/parquet", layout_detection=False)
```

The `delta` and `tables` methods take the same `layout_detection` argument.

</TabItem>
<TabItem value="r" label="R">

```r
data <- pc %>% pathling_read_parquet("/usr/share/staging/parquet",
                                     layout_detection = FALSE)
```

The `pathling_read_delta` and `pathling_read_tables` functions take the same
`layout_detection` argument.

</TabItem>
<TabItem value="scala" label="Scala">

```scala
val data = pc.read().withLayoutDetection(false)
        .parquet("/usr/share/staging/parquet")
```

</TabItem>
<TabItem value="java" label="Java">

```java
QueryableDataSource data = pc.read().withLayoutDetection(false)
        .parquet("/usr/share/staging/parquet");
```

</TabItem>
</Tabs>

## Writing FHIR data

Once you have read data in from a data source, you can also optionally write it
back out to a variety of targets. This is useful for persisting source data in a
more efficient form for query (e.g. Parquet or Delta), or for exporting data to
NDJSON for use in other systems.

Data is written in the Parquet on FHIR layout described in
[Storage layout](./schema.md). Writing into Parquet files, a Delta table or a
managed table that holds data written by an earlier release of Pathling is
refused when the write would add to that data (an append or a merge), with the
same message that [reading it](#layout-detection) gives. The check is made for
every resource type before anything is written, so a refused write leaves the
target as it was. A write that replaces the data, with the `overwrite` save
mode, is not refused, and is one way to re-import data into the same location.

### Save modes

Each write takes a save mode, which decides what happens to data that a
resource type already has at the target:

| Save mode   | Effect                                                                                                                            |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `error`     | The write fails if a resource type already has data at the target. For NDJSON and Parquet, nothing is written.                    |
| `overwrite` | The data of each resource type written replaces the data that type already has.                                                   |
| `append`    | The data is added beside the data each resource type already has.                                                                 |
| `ignore`    | A resource type that already has data is skipped, and its data is left as it is. Other resource types are written.                |
| `merge`     | Resources are matched on their ID: a matched resource is replaced, and a new one is added. Delta tables only, by path or managed. |

In Python and R, the save mode is passed as the `save_mode` argument, and
defaults to `error` for NDJSON and Parquet and to `overwrite` for Delta and
managed tables. In Java and Scala, it is set with the `saveMode` method of the
writer, and defaults to `error` for every target.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling.datasink import SaveMode

data.write.parquet("/usr/share/warehouse/parquet", save_mode=SaveMode.APPEND)
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_write_parquet("/usr/share/warehouse/parquet", save_mode = SaveMode$APPEND)
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
data.write().saveMode("append").parquet("/usr/share/warehouse/parquet")
```

</TabItem>
<TabItem value="java" label="Java">

```java
data.write().saveMode("append").parquet("/usr/share/warehouse/parquet");
```

</TabItem>
</Tabs>

### NDJSON

You can write data to a directory containing NDJSON files. Each resource type
is written to one or more files, named with the resource type, a partition
number and an identifier of the write
(`[resource type].[partition]-[write id].ndjson`), e.g.
`Patient.00000-7a60a041-0e7d-4e24-b23d-82405a665677.ndjson`. Because every write
has its own identifier, an append adds files beside those of earlier writes
rather than replacing them. The NDJSON source reads all of a type's files as
that type.

The save mode applies to the files a resource type already has in the
directory. With the default save mode, writing a resource type that already has
files fails with an error before any resource type is written. The `merge` save
mode is not supported for NDJSON.

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
containing [Parquet](https://parquet.apache.org/) files. Each resource type is
written to one or more files, named with the resource type, a partition number
and an identifier of the write (`[resource type].[partition]-[write id].parquet`),
e.g. `Patient.00000-7a60a041-0e7d-4e24-b23d-82405a665677.parquet`. Because every
write has its own identifier, an append adds files beside those of earlier
writes rather than replacing them. The Parquet source reads all of a type's
files as that type, including files named `[resource type].parquet` or
`[resource type].[partition].parquet`.

The save mode applies to the files a resource type already has in the
directory. With the default save mode, writing a resource type that already has
files fails with an error before any resource type is written. The `merge` save
mode is not supported for Parquet; use Delta if you need to merge.

Files appended to a resource type can carry columns that its earlier files do
not, because each write's schema is [fitted to its data](./schema.md). The
Parquet source [merges their schemas](#schema-merging) when it reads them.

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

The tables are named according to their resource
type (`[resource type].parquet`), e.g. `Patient.parquet`, `Condition.parquet`.

Because each write's schema is [fitted to its data](./schema.md), a batch can
carry elements that the table has not seen before. Appending or merging such a
batch widens the table to hold the new columns, rather than failing. A batch
that lacks columns the table carries can also be appended or merged: those
columns are left null on the rows that are written. In merge mode, a matched
resource is replaced in full, so an element that its earlier version carried and
the new version does not is left null rather than kept.

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

Appending to or merging into an existing Delta table widens it to hold columns
that only the new batch carries, as for [Delta Lake](#delta-lake-1). A table in
another format, such as Parquet, cannot be widened. Appending to such a table is
refused if the schema of the batch differs from the table's in any way,
including a batch that lacks some of the table's columns. Nothing is written to
the refused table, but tables of resource types written earlier in the same
write keep the data written to them.
Because each batch's schema is fitted to its data, this makes incremental loads
into a non-Delta managed table fail whenever the data changes shape. Write to a
Delta table instead, by choosing its [format](#table-format), or write Parquet
files to a path and read them with the [Parquet source](#parquet), which merges
the schemas of its files.

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

#### Table format

New tables are created in the format that the `spark.sql.sources.default`
setting of the Spark session names, which is Parquet unless your platform
configures otherwise. You can choose the format of the tables instead, for
example `delta` or `parquet`. The format is applied when a write creates a
table.

We recommend Delta for tables that will be appended to or merged into. Only a
Delta table can be widened to admit a batch with a different schema, and the
`merge` save mode works only with Delta tables, so a second merge into a table
that the first merge created in Parquet fails.

In Python and R, if you choose a format without specifying a schema, the tables
are written to the current schema of the Spark session.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
data.write.tables("test", save_mode="merge", format="delta")
```

</TabItem>
<TabItem value="r" label="R">

```r
data %>% ds_write_tables("test", save_mode = SaveMode$MERGE, format = "delta")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
data.write().saveMode("merge").tables("test", "delta")
```

</TabItem>
<TabItem value="java" label="Java">

```java
data.write().saveMode("merge").tables("test", "delta");
```

</TabItem>
</Tabs>
