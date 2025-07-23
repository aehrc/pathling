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

