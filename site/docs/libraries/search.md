---
sidebar_position: 5
description: The Pathling library provides functions for converting FHIR search expressions into Spark Columns for filtering resources.
---

# Search

The library provides functions for converting FHIR search expressions into Spark Columns. These columns can be used to filter resources based on search criteria defined in the FHIR specification.

Search parameters provide a standardised way to filter FHIR resources. For example, you can filter patients by gender, birth date, or active status using the same search syntax used in FHIR API queries.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## Basic filtering

The `search_to_column` function converts a FHIR search expression into a boolean Column that can be used with the `filter` operation.

In this example, we filter patients by gender using a simple search parameter.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients by gender.
gender_filter = pc.search_to_column("Patient", "gender=male")
patients.filter(gender_filter).select("id", "gender", "name.family").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients by gender using search syntax.
gender_filter <- pc_search_to_column(pc, "Patient", "gender=male")
sparklyr::spark_dataframe(patients) %>%
        sparklyr::j_invoke("filter", gender_filter) %>%
        sparklyr::sdf_register() %>%
        select(id, gender, name.family) %>%
        show()

# Alternatively, use the pathling_filter helper.
patients %>%
        pathling_filter(pc, "Patient", "gender=male", type = "search") %>%
        select(id, gender, name.family) %>%
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

// Filter patients by gender.
val genderFilter = pc.searchToColumn("Patient", "gender=male")
patients.filter(genderFilter).select("id", "gender", "name.family").show()
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

        // Filter patients by gender.
        Column genderFilter = pc.searchToColumn("Patient", "gender=male");
        patients.filter(genderFilter)
                .select("id", "gender", "name.family")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | gender | family   |
| ------------------------------------ | ------ | -------- |
| 8ee183e2-b3c0-4151-be94-b945d6aa8c6d | male   | Runte378 |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | male   | Smith    |

## Boolean logic

### AND logic

Multiple search parameters can be combined using `&`, which applies AND logic. All conditions must be satisfied for a resource to match.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients by gender AND active status.
combined_filter = pc.search_to_column("Patient", "gender=male&active=true")
patients.filter(combined_filter).select("id", "gender", "active").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients by gender AND active status.
patients %>%
        pathling_filter(pc, "Patient", "gender=male&active=true", type = "search") %>%
        select(id, gender, active) %>%
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

// Filter patients by gender AND active status.
val combinedFilter = pc.searchToColumn("Patient", "gender=male&active=true")
patients.filter(combinedFilter).select("id", "gender", "active").show()
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

        // Filter patients by gender AND active status.
        Column combinedFilter = pc.searchToColumn("Patient", "gender=male&active=true");
        patients.filter(combinedFilter)
                .select("id", "gender", "active")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | gender | active |
| ------------------------------------ | ------ | ------ |
| 8ee183e2-b3c0-4151-be94-b945d6aa8c6d | male   | true   |

### OR logic

Multiple values for the same parameter can be combined using commas, which applies OR logic. A resource matches if any of the values match.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients with gender male OR female.
or_filter = pc.search_to_column("Patient", "gender=male,female")
patients.filter(or_filter).select("id", "gender").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients with gender male OR female.
patients %>%
        pathling_filter(pc, "Patient", "gender=male,female", type = "search") %>%
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

// Filter patients with gender male OR female.
val orFilter = pc.searchToColumn("Patient", "gender=male,female")
patients.filter(orFilter).select("id", "gender").show()
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

        // Filter patients with gender male OR female.
        Column orFilter = pc.searchToColumn("Patient", "gender=male,female");
        patients.filter(orFilter)
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
| 7b4d8c2f-9a3e-4d5b-8c1f-2e3d4c5b6a7d | female |

## Comparison prefixes

Search parameters support prefixes for comparisons on dates, numbers, and quantities. The following prefixes are supported:

- `eq` (equal, default)
- `ne` (not equal)
- `lt` (less than)
- `le` (less than or equal)
- `gt` (greater than)
- `ge` (greater than or equal)

### Date comparisons

Prefixes can be applied to date search parameters to filter based on temporal relationships.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients born on or after 1990-01-01.
date_filter = pc.search_to_column("Patient", "birthdate=ge1990-01-01")
patients.filter(date_filter).select("id", "birthDate").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients born on or after 1990-01-01.
patients %>%
        pathling_filter(pc, "Patient", "birthdate=ge1990-01-01", type = "search") %>%
        select(id, birthDate) %>%
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

// Filter patients born on or after 1990-01-01.
val dateFilter = pc.searchToColumn("Patient", "birthdate=ge1990-01-01")
patients.filter(dateFilter).select("id", "birthDate").show()
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

        // Filter patients born on or after 1990-01-01.
        Column dateFilter = pc.searchToColumn("Patient", "birthdate=ge1990-01-01");
        patients.filter(dateFilter)
                .select("id", "birthDate")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | birthDate  |
| ------------------------------------ | ---------- |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | 1995-06-15 |

### Quantity comparisons

Prefixes also apply to quantity parameters, enabling filtering based on numeric values with units.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
observations = data_source.read("Observation")

# Filter observations with value greater than or equal to 80 mmHg.
quantity_filter = pc.search_to_column("Observation", "value-quantity=ge80|http://unitsofmeasure.org|mm[Hg]")
observations.filter(quantity_filter).select("id", "code.coding.code", "valueQuantity.value", "valueQuantity.unit").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
observations <- data_source %>% ds_read("Observation")

# Filter observations with value greater than or equal to 80 mmHg.
observations %>%
        pathling_filter(pc, "Observation", "value-quantity=ge80|http://unitsofmeasure.org|mm[Hg]", type = "search") %>%
        select(id, code.coding.code, valueQuantity.value, valueQuantity.unit) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val observations = dataSource.read("Observation")

// Filter observations with value greater than or equal to 80 mmHg.
val quantityFilter = pc.searchToColumn("Observation", "value-quantity=ge80|http://unitsofmeasure.org|mm[Hg]")
observations.filter(quantityFilter).select("id", "code.coding.code", "valueQuantity.value", "valueQuantity.unit").show()
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
        Dataset<Row> observations = dataSource.read("Observation");

        // Filter observations with value greater than or equal to 80 mmHg.
        Column quantityFilter = pc.searchToColumn("Observation", "value-quantity=ge80|http://unitsofmeasure.org|mm[Hg]");
        observations.filter(quantityFilter)
                .select("id", "code.coding.code", "valueQuantity.value", "valueQuantity.unit")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | code    | value | unit   |
| ------------------------------------ | ------- | ----- | ------ |
| 1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p | 85354-9 | 120.0 | mm[Hg] |
| 2b3c4d5e-6f7g-8h9i-0j1k-2l3m4n5o6p7q | 8480-6  | 90.0  | mm[Hg] |

## Search parameter types

Different FHIR search parameter types support different matching behaviours.

### Quantity parameters

Quantity parameters match numeric values with units. The syntax is `[prefix]value|system|code` where the system and code identify the unit.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
observations = data_source.read("Observation")

# Filter observations by quantity with specific unit.
quantity_filter = pc.search_to_column("Observation", "value-quantity=5.4|http://unitsofmeasure.org|mmol/L")
observations.filter(quantity_filter).select("id", "valueQuantity.value", "valueQuantity.unit").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
observations <- data_source %>% ds_read("Observation")

# Filter observations by quantity with specific unit.
observations %>%
        pathling_filter(pc, "Observation", "value-quantity=5.4|http://unitsofmeasure.org|mmol/L", type = "search") %>%
        select(id, valueQuantity.value, valueQuantity.unit) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val observations = dataSource.read("Observation")

// Filter observations by quantity with specific unit.
val quantityFilter = pc.searchToColumn("Observation", "value-quantity=5.4|http://unitsofmeasure.org|mmol/L")
observations.filter(quantityFilter).select("id", "valueQuantity.value", "valueQuantity.unit").show()
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
        Dataset<Row> observations = dataSource.read("Observation");

        // Filter observations by quantity with specific unit.
        Column quantityFilter = pc.searchToColumn("Observation", "value-quantity=5.4|http://unitsofmeasure.org|mmol/L");
        observations.filter(quantityFilter)
                .select("id", "valueQuantity.value", "valueQuantity.unit")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | value | unit   |
| ------------------------------------ | ----- | ------ |
| 3c4d5e6f-7g8h-9i0j-1k2l-3m4n5o6p7q8r | 5.4   | mmol/L |

### String parameters

String parameters perform case-insensitive partial matching by default. The search value matches if it appears anywhere within the target string.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients by family name containing "smith".
name_filter = pc.search_to_column("Patient", "family=smith")
patients.filter(name_filter).select("id", "name.family").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients by family name containing "smith".
patients %>%
        pathling_filter(pc, "Patient", "family=smith", type = "search") %>%
        select(id, name.family) %>%
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

// Filter patients by family name containing "smith".
val nameFilter = pc.searchToColumn("Patient", "family=smith")
patients.filter(nameFilter).select("id", "name.family").show()
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

        // Filter patients by family name containing "smith".
        Column nameFilter = pc.searchToColumn("Patient", "family=smith");
        patients.filter(nameFilter)
                .select("id", "name.family")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | family    |
| ------------------------------------ | --------- |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | Smith     |
| 4d5e6f7g-8h9i-0j1k-2l3m-4n5o6p7q8r9s | Goldsmith |

### Reference parameters

Reference parameters filter resources based on references to other resources. The value can be a resource ID or a full reference.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
observations = data_source.read("Observation")

# Filter observations by patient reference.
ref_filter = pc.search_to_column("Observation", "subject=Patient/8ee183e2-b3c0-4151-be94-b945d6aa8c6d")
observations.filter(ref_filter).select("id", "subject.reference").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
observations <- data_source %>% ds_read("Observation")

# Filter observations by patient reference.
observations %>%
        pathling_filter(pc, "Observation", "subject=Patient/8ee183e2-b3c0-4151-be94-b945d6aa8c6d", type = "search") %>%
        select(id, subject.reference) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val observations = dataSource.read("Observation")

// Filter observations by patient reference.
val refFilter = pc.searchToColumn("Observation", "subject=Patient/8ee183e2-b3c0-4151-be94-b945d6aa8c6d")
observations.filter(refFilter).select("id", "subject.reference").show()
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
        Dataset<Row> observations = dataSource.read("Observation");

        // Filter observations by patient reference.
        Column refFilter = pc.searchToColumn("Observation", "subject=Patient/8ee183e2-b3c0-4151-be94-b945d6aa8c6d");
        observations.filter(refFilter)
                .select("id", "subject.reference")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | reference                                    |
| ------------------------------------ | -------------------------------------------- |
| 5e6f7g8h-9i0j-1k2l-3m4n-5o6p7q8r9s0t | Patient/8ee183e2-b3c0-4151-be94-b945d6aa8c6d |

### Number parameters

Number parameters match numeric values without units. Prefixes can be used for range comparisons.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
risk_assessments = data_source.read("RiskAssessment")

# Filter risk assessments by probability.
number_filter = pc.search_to_column("RiskAssessment", "probability=gt0.5")
risk_assessments.filter(number_filter).select("id", "prediction.probabilityDecimal").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
risk_assessments <- data_source %>% ds_read("RiskAssessment")

# Filter risk assessments by probability.
risk_assessments %>%
        pathling_filter(pc, "RiskAssessment", "probability=gt0.5", type = "search") %>%
        select(id, prediction.probabilityDecimal) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
val dataSource = pc.read.ndjson("data/ndjson")
val riskAssessments = dataSource.read("RiskAssessment")

// Filter risk assessments by probability.
val numberFilter = pc.searchToColumn("RiskAssessment", "probability=gt0.5")
riskAssessments.filter(numberFilter).select("id", "prediction.probabilityDecimal").show()
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
        Dataset<Row> riskAssessments = dataSource.read("RiskAssessment");

        // Filter risk assessments by probability.
        Column numberFilter = pc.searchToColumn("RiskAssessment", "probability=gt0.5");
        riskAssessments.filter(numberFilter)
                .select("id", "prediction.probabilityDecimal")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | probabilityDecimal |
| ------------------------------------ | ------------------ |
| 6f7g8h9i-0j1k-2l3m-4n5o-6p7q8r9s0t1u | 0.75               |

### URI parameters

URI parameters match Uniform Resource Identifiers exactly. These are commonly used for identifiers, profiles, and code system URIs.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients by identifier system.
uri_filter = pc.search_to_column("Patient", "identifier=http://example.org/fhir/identifier|")
patients.filter(uri_filter).select("id", "identifier.system", "identifier.value").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients by identifier system.
patients %>%
        pathling_filter(pc, "Patient", "identifier=http://example.org/fhir/identifier|", type = "search") %>%
        select(id, identifier.system, identifier.value) %>%
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

// Filter patients by identifier system.
val uriFilter = pc.searchToColumn("Patient", "identifier=http://example.org/fhir/identifier|")
patients.filter(uriFilter).select("id", "identifier.system", "identifier.value").show()
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

        // Filter patients by identifier system.
        Column uriFilter = pc.searchToColumn("Patient", "identifier=http://example.org/fhir/identifier|");
        patients.filter(uriFilter)
                .select("id", "identifier.system", "identifier.value")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | system                             | value     |
| ------------------------------------ | ---------------------------------- | --------- |
| 8ee183e2-b3c0-4151-be94-b945d6aa8c6d | http://example.org/fhir/identifier | MRN123456 |

## Search modifiers

Modifiers alter the behaviour of search parameters. They are appended to the parameter name using a colon.

### :not modifier

The `:not` modifier negates the search condition, matching resources where the parameter does NOT have the specified value.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients where gender is NOT male.
not_filter = pc.search_to_column("Patient", "gender:not=male")
patients.filter(not_filter).select("id", "gender").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients where gender is NOT male.
patients %>%
        pathling_filter(pc, "Patient", "gender:not=male", type = "search") %>%
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

// Filter patients where gender is NOT male.
val notFilter = pc.searchToColumn("Patient", "gender:not=male")
patients.filter(notFilter).select("id", "gender").show()
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

        // Filter patients where gender is NOT male.
        Column notFilter = pc.searchToColumn("Patient", "gender:not=male");
        patients.filter(notFilter)
                .select("id", "gender")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | gender  |
| ------------------------------------ | ------- |
| 7b4d8c2f-9a3e-4d5b-8c1f-2e3d4c5b6a7d | female  |
| 9c0d1e2f-3a4b-5c6d-7e8f-9g0h1i2j3k4l | unknown |

### :exact modifier

The `:exact` modifier changes string matching from case-insensitive partial matching to case-sensitive exact matching.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients with exact family name "Smith".
exact_filter = pc.search_to_column("Patient", "family:exact=Smith")
patients.filter(exact_filter).select("id", "name.family").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients with exact family name "Smith".
patients %>%
        pathling_filter(pc, "Patient", "family:exact=Smith", type = "search") %>%
        select(id, name.family) %>%
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

// Filter patients with exact family name "Smith".
val exactFilter = pc.searchToColumn("Patient", "family:exact=Smith")
patients.filter(exactFilter).select("id", "name.family").show()
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

        // Filter patients with exact family name "Smith".
        Column exactFilter = pc.searchToColumn("Patient", "family:exact=Smith");
        patients.filter(exactFilter)
                .select("id", "name.family")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | family |
| ------------------------------------ | ------ |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | Smith  |

Note that "smith" (lowercase) and "Goldsmith" would not match with the `:exact` modifier.

## FHIRPath expressions

For more complex filtering requirements beyond what search parameters support, you can use FHIRPath expressions directly.

### Using fhirpath_to_column

The `fhirpath_to_column` method provides direct access to the FHIRPath engine, allowing you to evaluate arbitrary FHIRPath expressions against resources.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Filter patients using FHIRPath expression.
fhirpath_filter = pc.fhirpath_to_column("Patient", "name.family contains 'Smith'")
patients.filter(fhirpath_filter).select("id", "name.family").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Filter patients using FHIRPath expression.
fhirpath_filter <- pc_fhirpath_to_column(pc, "Patient", "name.family contains 'Smith'")
sparklyr::spark_dataframe(patients) %>%
        sparklyr::j_invoke("filter", fhirpath_filter) %>%
        sparklyr::sdf_register() %>%
        select(id, name.family) %>%
        show()

# Alternatively, use the pathling_filter helper.
patients %>%
        pathling_filter(pc, "Patient", "name.family contains 'Smith'", type = "fhirpath") %>%
        select(id, name.family) %>%
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

// Filter patients using FHIRPath expression.
val fhirpathFilter = pc.fhirpathToColumn("Patient", "name.family contains 'Smith'")
patients.filter(fhirpathFilter).select("id", "name.family").show()
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

        // Filter patients using FHIRPath expression.
        Column fhirpathFilter = pc.fhirpathToColumn("Patient", "name.family contains 'Smith'");
        patients.filter(fhirpathFilter)
                .select("id", "name.family")
                .show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| id                                   | family    |
| ------------------------------------ | --------- |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | Smith     |
| 4d5e6f7g-8h9i-0j1k-2l3m-4n5o6p7q8r9s | Goldsmith |

## Combining filters

Multiple search column expressions can be combined using boolean operators to create complex filter conditions.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Create separate filters.
male_filter = pc.search_to_column("Patient", "gender=male")
female_filter = pc.search_to_column("Patient", "gender=female")

# Combine with OR logic using | operator.
gender_filter = male_filter | female_filter

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

# Chain multiple filter operations.
patients %>%
        pathling_filter(pc, "Patient", "gender=male", type = "search") %>%
        pathling_filter(pc, "Patient", "birthdate=ge1990-01-01", type = "search") %>%
        select(id, gender, birthDate) %>%
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

// Create separate filters.
val maleFilter = pc.searchToColumn("Patient", "gender=male")
val femaleFilter = pc.searchToColumn("Patient", "gender=female")

// Combine with OR logic using || operator.
val genderFilter = maleFilter || femaleFilter

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

        // Create separate filters.
        Column maleFilter = pc.searchToColumn("Patient", "gender=male");
        Column femaleFilter = pc.searchToColumn("Patient", "gender=female");

        // Combine with OR logic using or() method.
        Column genderFilter = maleFilter.or(femaleFilter);

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
| 7b4d8c2f-9a3e-4d5b-8c1f-2e3d4c5b6a7d | female |

## Empty query

An empty search expression matches all resources, which is useful for dynamic filtering scenarios where the filter may be conditionally applied.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
data_source = pc.read.ndjson("data/ndjson")
patients = data_source.read("Patient")

# Empty search expression matches all resources.
all_filter = pc.search_to_column("Patient", "")
patients.filter(all_filter).select("id", "gender").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
data_source <- pc %>% pathling_read_ndjson("data/ndjson")
patients <- data_source %>% ds_read("Patient")

# Empty search expression matches all resources.
patients %>%
        pathling_filter(pc, "Patient", "", type = "search") %>%
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

// Empty search expression matches all resources.
val allFilter = pc.searchToColumn("Patient", "")
patients.filter(allFilter).select("id", "gender").show()
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

        // Empty search expression matches all resources.
        Column allFilter = pc.searchToColumn("Patient", "");
        patients.filter(allFilter)
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
| 7b4d8c2f-9a3e-4d5b-8c1f-2e3d4c5b6a7d | female |
| 93ee0b14-4f22-4c1a-93e2-b4e5c0d7f0d6 | male   |
