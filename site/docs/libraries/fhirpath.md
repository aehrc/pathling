---
sidebar_position: 6
description: The Pathling library provides functions for converting FHIRPath expressions into Spark Columns for filtering and extracting data.
---

# FHIRPath

The library provides functions for converting FHIRPath expressions into Spark Columns. These columns can be used for filtering resources with boolean expressions or extracting values from complex FHIR data structures.

FHIRPath is a path-based navigation and extraction language designed specifically for FHIR data. It provides more expressive power than search parameters, enabling complex queries and data extraction operations.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## Boolean filtering

FHIRPath expressions that evaluate to boolean values can be used with the `filter` operation to select resources that match specific criteria.

In this example, we filter patients by gender using a FHIRPath boolean expression.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients using a boolean FHIRPath expression.
gender_filter = pc.fhirpath_to_column("Patient", "gender = 'male'")
patients.filter(gender_filter).select("id", "gender").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients using a boolean FHIRPath expression.
gender_filter <- pathling_fhirpath_to_column(pc, "Patient", "gender = 'male'")
sparklyr::spark_dataframe(patients) %>%
        sparklyr::j_invoke("filter", gender_filter) %>%
        sparklyr::sdf_register() %>%
        select(id, gender) %>%
        show()

# Alternatively, use the pathling_filter helper.
patients %>%
        pathling_filter(pc, "Patient", "gender = 'male'") %>%
        select(id, gender) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val patients = dataSource.read("Patient")

// Filter patients using a boolean FHIRPath expression.
val genderFilter = pc.fhirPathToColumn("Patient", "gender = 'male'")
patients.filter(genderFilter).select("id", "gender").show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        DataSource dataSource = pc.read().ndjson("data/ndjson");
        Dataset<Row> patients = dataSource.read("Patient");

        // Filter patients using a boolean FHIRPath expression.
        Column genderFilter = pc.fhirPathToColumn("Patient", "gender = 'male'");
        patients.filter(genderFilter)
                .select("id", "gender")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | gender |
| ------------------------------------ | ------ |
| 8ee183e2-b3c0-4151-be94-b945d6aa8c6d | male   |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | male   |

## Value extraction

FHIRPath expressions can extract values from FHIR resources. These expressions can be used with `select` or `withColumn` operations to add derived columns to your dataset.

The expression should evaluate to a single value per resource to avoid collection-valued results.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Extract the first given name using FHIRPath.
first_name = pc.fhirpath_to_column("Patient", "name.given.first()")
patients.withColumn("given_name", first_name).select("id", "given_name").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Extract the first given name using FHIRPath.
patients %>%
        pathling_with_column(pc, "Patient", "name.given.first()", column = "given_name") %>%
        select(id, given_name) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val patients = dataSource.read("Patient")

// Extract the first given name using FHIRPath.
val firstName = pc.fhirPathToColumn("Patient", "name.given.first()")
patients.withColumn("given_name", firstName).select("id", "given_name").show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        DataSource dataSource = pc.read().ndjson("data/ndjson");
        Dataset<Row> patients = dataSource.read("Patient");

        // Extract the first given name using FHIRPath.
        Column firstName = pc.fhirPathToColumn("Patient", "name.given.first()");
        patients.withColumn("given_name", firstName)
                .select("id", "given_name")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | given_name |
| ------------------------------------ | ---------- |
| 8ee183e2-b3c0-4151-be94-b945d6aa8c6d | Collin     |
| 7b4d8c2f-9a3e-4d5b-8c1f-2e3d4c5b6a7d | Patrica    |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | John       |

## Complex expressions

FHIRPath supports complex expressions including path traversal, filtering, and function calls. This enables sophisticated queries that would be difficult to express with search parameters alone.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Extract the family name from official names only.
official_family = pc.fhirpath_to_column(
    "Patient",
    "name.where(use = 'official').family.first()"
)
patients.withColumn("official_family", official_family).select(
    "id", "official_family"
).show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Extract the family name from official names only.
patients %>%
        pathling_with_column(
                pc,
                "Patient",
                "name.where(use = 'official').family.first()",
                column = "official_family"
        ) %>%
        select(id, official_family) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val patients = dataSource.read("Patient")

// Extract the family name from official names only.
val officialFamily = pc.fhirPathToColumn(
    "Patient",
    "name.where(use = 'official').family.first()"
)
patients.withColumn("official_family", officialFamily)
    .select("id", "official_family")
    .show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        DataSource dataSource = pc.read().ndjson("data/ndjson");
        Dataset<Row> patients = dataSource.read("Patient");

        // Extract the family name from official names only.
        Column officialFamily = pc.fhirPathToColumn(
            "Patient",
            "name.where(use = 'official').family.first()"
        );
        patients.withColumn("official_family", officialFamily)
                .select("id", "official_family")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | official_family |
| ------------------------------------ | --------------- |
| 8ee183e2-b3c0-4151-be94-b945d6aa8c6d | Runte378        |
| 7b4d8c2f-9a3e-4d5b-8c1f-2e3d4c5b6a7d | Donnelly735     |

## Combining with other operations

FHIRPath columns can be integrated into larger transformation pipelines, combining filtering and value extraction with other DataFrame operations.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Build a pipeline that filters and extracts data.
gender_filter = pc.fhirpath_to_column("Patient", "gender = 'male'")
birth_year = pc.fhirpath_to_column("Patient", "birthDate.toString().substring(0, 4)")

result = (
    patients
    .filter(gender_filter)
    .withColumn("birth_year", birth_year)
    .select("id", "gender", "birth_year")
    .orderBy("birth_year")
)
result.show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Build a pipeline that filters and extracts data.
result <- patients %>%
        pathling_filter(pc, "Patient", "gender = 'male'") %>%
        pathling_with_column(
                pc,
                "Patient",
                "birthDate.toString().substring(0, 4)",
                column = "birth_year"
        ) %>%
        select(id, gender, birth_year) %>%
        arrange(birth_year)

result %>% show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val patients = dataSource.read("Patient")

// Build a pipeline that filters and extracts data.
val genderFilter = pc.fhirPathToColumn("Patient", "gender = 'male'")
val birthYear = pc.fhirPathToColumn("Patient", "birthDate.toString().substring(0, 4)")

val result = patients
    .filter(genderFilter)
    .withColumn("birth_year", birthYear)
    .select("id", "gender", "birth_year")
    .orderBy("birth_year")

result.show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        DataSource dataSource = pc.read().ndjson("data/ndjson");
        Dataset<Row> patients = dataSource.read("Patient");

        // Build a pipeline that filters and extracts data.
        Column genderFilter = pc.fhirPathToColumn("Patient", "gender = 'male'");
        Column birthYear = pc.fhirPathToColumn(
            "Patient",
            "birthDate.toString().substring(0, 4)"
        );

        Dataset<Row> result = patients
                .filter(genderFilter)
                .withColumn("birth_year", birthYear)
                .select("id", "gender", "birth_year")
                .orderBy("birth_year");

        result.show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | gender | birth_year |
| ------------------------------------ | ------ | ---------- |
| 8ee183e2-b3c0-4151-be94-b945d6aa8c6d | male   | 1967       |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | male   | 1995       |
