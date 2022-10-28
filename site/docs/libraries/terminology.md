---
sidebar_position: 3
---

# Terminology functions

The library also provides a set of functions for querying a FHIR terminology
server from within your queries and transformations.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import {
JavaInstallation,
PythonInstallation,
ScalaInstallation
} from "../../src/components/installation";

### Value set membership

The `member_of` function can be used to test the membership of a code within a
FHIR value set. In this example, we take a list of SNOMED CT diagnosis codes and
create a new column which shows which are viral infections.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

<PythonInstallation/>

```python
from pathling import PathlingContext
from pathling.functions import to_coding, to_ecl_value_set

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

result = pc.member_of(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      to_ecl_value_set("""
<< 64572001|Disease| : (
  << 370135005|Pathological process| = << 441862004|Infectious process|,
  << 246075003|Causative agent| = << 49872002|Virus|
)
                      """), 'VIRAL_INFECTION')
result.select('CODE', 'DESCRIPTION', 'VIRAL_INFECTION').show()
```

</TabItem>
<TabItem value="scala" label="Scala">

<ScalaInstallation/>

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.library.TerminologyHelpers._

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

val result = pc.memberOf(csv, toCoding(csv.col("CODE"), "http://snomed.info/sct"),
    toEclValueSet("""
        << 64572001|Disease| : (
          << 370135005|Pathological process| = << 441862004|Infectious process|,
          << 246075003|Causative agent| = << 49872002|Virus|
        )
    """), "VIRAL_INFECTION")
result.select("CODE", "DESCRIPTION", "VIRAL_INFECTION").show()
```

</TabItem>
<TabItem value="java" label="Java">

<JavaInstallation/>

```java
import static au.csiro.pathling.library.TerminologyHelpers.*;
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {
    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        Dataset<Row> result = pc.memberOf(csv, toCoding(csv.col("code"), SNOMED_URI),
            toEclValueSet("<< 64572001|Disease| : ("
                + "<< 370135005|Pathological process| = << 441862004|Infectious process|,"
                + "<< 246075003|Causative agent| = << 49872002|Virus|"
                + ")"), "VIRAL_INFECTION");
        result.select("CODE", "DESCRIPTION", "VIRAL_INFECTION").show();
    }
}

```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION               | VIRAL_INFECTION |
|-----------|---------------------------|-----------------|
| 65363002  | Otitis media              | false           |
| 16114001  | Fracture of ankle         | false           |
| 444814009 | Viral sinusitis           | true            |
| 444814009 | Viral sinusitis           | true            |
| 43878008  | Streptococcal sore throat | false           |

### Concept translation

The `translate` function can be used to translate codes from one code system to
another using maps that are known to the terminology server. In this example, we
translate our SNOMED CT diagnosis codes into Read CTV3.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

<PythonInstallation/>

```python
from pathling import PathlingContext
from pathling.functions import to_coding

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

result = pc.translate(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      'http://snomed.info/sct/900000000000207008?fhir_cm='
                      '900000000000497000',
                      output_column_name='READ_CODE')
result = result.withColumn('READ_CODE', result.READ_CODE.code)
result.select('CODE', 'DESCRIPTION', 'READ_CODE').show()
```

</TabItem>
<TabItem value="scala" label="Scala">

<ScalaInstallation/>

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.library.TerminologyHelpers._

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

val result = pc.translate(csv, toCoding(csv.col("CODE"), SNOMED_URI),
    "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000", 
    false, "equivalent", "READ_CODE")
result.select("CODE", "DESCRIPTION", "READ_CODE").show()
```

</TabItem>
<TabItem value="java" label="Java">

<JavaInstallation/>

```java
import static au.csiro.pathling.library.TerminologyHelpers.*;
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {
    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        Dataset<Row> result = pc.translate(csv, toCoding(csv.col("CODE"), SNOMED_URI),
            "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000", 
            false, "equivalent", "READ_CODE");
        result.select("CODE", "DESCRIPTION", "READ_CODE").show();
    }
}

```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION               | READ_CODE |
|-----------|---------------------------|-----------|
| 65363002  | Otitis media              | X00ik     |
| 16114001  | Fracture of ankle         | S34..     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 43878008  | Streptococcal sore throat | A340.     |

### Subsumption testing

The `subsumes` function allows us to perform subsumption testing on codes within
our data. In hierarchical code systems, a subsumption test determines whether a
code is a subtype of another code, e.g. an "ankle fracture" is subsumed by "
fracture".

In this example, we first take our codes, cross-join them and then test whether
they subsume each other. Then we do another subsumption test against the "ear,
nose and throat disorder" concept.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

<PythonInstallation/>

```python
from pathling import PathlingContext
from pathling.coding import Coding
from pathling.functions import to_coding

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

# 232208008 |Ear, nose and throat disorder|
left_coding = Coding('http://snomed.info/sct', '232208008')
right_coding_column = to_coding(csv.CODE, 'http://snomed.info/sct')

result = pc.subsumes(csv, 'SUBSUMES',
                     left_coding=left_coding,
                     right_coding_column=right_coding_column)

result.select('CODE', 'DESCRIPTION', 'IS_ENT').show()
```

</TabItem>
<TabItem value="scala" label="Scala">

<ScalaInstallation/>

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.library.TerminologyHelpers._
import au.csiro.pathling.fhirpath.encoding.CodingEncoding

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

val result = pc.subsumes(csv,
    // 232208008 |Ear, nose and throat disorder|
    CodingEncoding.toStruct(
        lit(null),
        lit(SNOMED_URI),
        lit(null),
        lit("232208008"),
        lit(null),
        lit(null)
    ), toCoding(csv.col("CODE"), SNOMED_URI), "IS_ENT")
result.select("CODE", "DESCRIPTION", "IS_ENT").show()
```

</TabItem>
<TabItem value="java" label="Java">

<JavaInstallation/>

```java
import static au.csiro.pathling.library.TerminologyHelpers.*;
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {
    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        Dataset<Row> result = pc.subsumes(csv,
            // 232208008 |Ear, nose and throat disorder|
            CodingEncoding.toStruct(
                lit(null),
                lit(SNOMED_URI),
                lit(null),
                lit("232208008"),
                lit(null),
                lit(null)
            ), toCoding(csv.col("CODE"), SNOMED_URI), "IS_ENT");
        result.select("CODE", "DESCRIPTION", "IS_ENT").show();
    }
}

```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION       | IS_ENT |
|-----------|-------------------|--------|
| 65363002  | Otitis media      | true   |
| 16114001  | Fracture of ankle | false  |
| 444814009 | Viral sinusitis   | true   |

### Authentication

Pathling can be configured to connect to a protected terminology server by
supplying a set of OAuth2 client credentials and a token endpoint.

Here is an example of how to authenticate to
the [NHS terminology server](https://ontology.nhs.uk/):

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

<PythonInstallation/>

```python
from pathling import PathlingContext

pc = PathlingContext.create(
    terminology_server_url='https://ontology.nhs.uk/production1/fhir',
    token_endpoint='https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token',
    client_id='[client ID]',
    client_secret='[client secret]'
)
```

</TabItem>
<TabItem value="scala" label="Scala">

<ScalaInstallation/>

```scala
import au.csiro.pathling.library.{PathlingContext, PathlingContextConfiguration}

val config = PathlingContextConfiguration.builder()
        .terminologyServerUrl("https://ontology.nhs.uk/production1/fhir")
        .tokenEndpoint("https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token")
        .clientId("[client ID]")
        .clientSecret("[client secret]")
        .build()
val pc = PathlingContext.create(config)
```

</TabItem>
<TabItem value="java" label="Java">

<JavaInstallation/>

```java
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.PathlingContextConfiguration;

class MyApp {
    public static void main(String[] args) {
        PathlingContextConfiguration config = PathlingContextConfiguration.builder()
                .terminologyServerUrl("https://ontology.nhs.uk/production1/fhir")
                .tokenEndpoint("https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token")
                .clientId("[client ID]")
                .clientSecret("[client secret]")
                .build();
        PathlingContext pc = PathlingContext.create(config);
        // ...
    }
}

```

</TabItem>
</Tabs>
