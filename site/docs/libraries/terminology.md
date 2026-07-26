---
sidebar_position: 4
description: The Pathling library provides a set of functions for querying a FHIR terminology server from within your queries and transformations.
---

# Terminology functions

The library also provides a set of functions for querying a FHIR terminology
server from within your queries and transformations.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

### Value set membership

The `member_of` function can be used to test the membership of a code within a
[FHIR value set](https://hl7.org/fhir/valueset.html). This can be used with both
explicit value sets (i.e. those that have been pre-defined and loaded into the
terminology server) and implicit value sets (e.g. SNOMED CT
[Expression Constraint Language](http://snomed.org/ecl)).

In this example, we take a list of SNOMED CT diagnosis codes and create a new
column which shows which are viral infections. We use an ECL expression to
define viral infection as a disease with a pathological process of "Infectious
process", and a causative agent of "Virus".

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, to_snomed_coding, to_ecl_value_set, member_of

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

VIRAL_INFECTION_ECL = """
    << 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )
"""

csv.select(
        "CODE",
        "DESCRIPTION",
        member_of(
                to_snomed_coding(csv.CODE),
                to_ecl_value_set(VIRAL_INFECTION_ECL)
        ).alias("VIRAL_INFECTION"),
).show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
csv <- pathling_spark(pc) %>%
        spark_read_csv(path = 'conditions.csv', header = TRUE)

VIRAL_DISEASE_ECL <- '<< 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )'

csv %>%
        mutate(
                CODE,
                DESCRIPTION,
                IS_VIRAL_DISEASE = !!tx_member_of(!!tx_to_snomed_coding(CODE), !!tx_to_ecl_value_set(VIRAL_DISEASE_ECL)),
                .keep = "none"
        ) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.sql.Terminology._
import au.csiro.pathling.library.TerminologyHelpers._

val pc = PathlingContext.create()
val csv = pc.getSpark.read.csv("conditions.csv")

val VIRAL_INFECTION_ECL =
    """
    << 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )
"""

csv.select(
    csv.col("CODE"),
    csv.col("DESCRIPTION"),
    member_of(toCoding(csv.col("CODE"), "http://snomed.info/sct"),
        toEclValueSet(VIRAL_INFECTION_ECL)).alias("VIRAL_INFECTION")
).show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import static au.csiro.pathling.library.TerminologyHelpers.*;
import static au.csiro.pathling.sql.Terminology.*;

import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        String VIRAL_INFECTION_ECL = """
                    << 64572001|Disease| : (
                      << 370135005|Pathological process| = << 441862004|Infectious process|,
                      << 246075003|Causative agent| = << 49872002|Virus|
                    )
                """;

        csv.select(
                csv.col("CODE"),
                csv.col("DESCRIPTION"),
                member_of(toSnomedCoding(csv.col("CODE")),
                        toEclValueSet(VIRAL_INFECTION_ECL)).alias(
                        "VIRAL_INFECTION")
        ).show();
    }
}

```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION               | VIRAL_INFECTION |
| --------- | ------------------------- | --------------- |
| 65363002  | Otitis media              | false           |
| 16114001  | Fracture of ankle         | false           |
| 444814009 | Viral sinusitis           | true            |
| 444814009 | Viral sinusitis           | true            |
| 43878008  | Streptococcal sore throat | false           |

### Concept translation

The `translate` function can be used to translate codes from one code system to
another using maps that are known to the terminology server. In this example, we
translate our SNOMED CT diagnosis codes
into [Read CTV3](https://digital.nhs.uk/services/terminology-and-classifications/read-codes).

Please note that the
type of the output column is the array of coding structs, as the translation may
produce multiple results for each input coding.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, to_snomed_coding, translate
from pyspark.sql.functions import explode_outer

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

translate_result = csv.withColumn(
        "READ_CODES",
        translate(
                to_snomed_coding(csv.CODE),
                concept_map_uri="http://snomed.info/sct/900000000000207008?"
                                "fhir_cm=900000000000497000",
        ).code,
)
translate_result.select(
        "CODE", "DESCRIPTION", explode_outer("READ_CODES").alias("READ_CODE")
).show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
csv <- pathling_spark(pc) %>%
        spark_read_csv(path = 'conditions.csv', header = TRUE)

translate_result <- csv %>%
        mutate(
                READ_CODES = !!tx_translate(!!tx_to_snomed_coding(CODE),
                                             concept_map_uri = "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000")
        ) %>%
        mutate(
                READ_CODES = explode_outer(READ_CODES[['code']])
        ) %>%
        select(CODE, DESCRIPTION, READ_CODES) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.sql.Terminology._
import au.csiro.pathling.library.TerminologyHelpers._
import org.apache.spark.sql.functions.explode_outer

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

val translate_result = csv.withColumn(
    "READ_CODES",
    translate(
        toCoding(csv.col("CODE"), "https://snomed.info/sct"),
        "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000",
        false, null
    ).getField("code")
)
translate_result.select(
    csv.col("CODE"), csv.col("DESCRIPTION"), explode_outer(translate_result.col("READ_CODES")).alias("READ_CODE")
).show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import static au.csiro.pathling.sql.Terminology.*;
import static au.csiro.pathling.library.TerminologyHelpers.*;
import static org.apache.spark.sql.functions.explode_outer;

import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        Dataset<Row> translateResult = csv.withColumn(
                "READ_CODES",
                translate(
                        toCoding(csv.col("CODE"), "https://snomed.info/sct"),
                        "http://snomed.info/sct/900000000000207008?fhir_cm=900000000000497000",
                        false, null
                ).getField("code")
        );
        translateResult.select(
                csv.col("CODE"), csv.col("DESCRIPTION"),
                explode_outer(translate_result.col("READ_CODES")).alias(
                        "READ_CODE")
        ).show();
    }
}

```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION               | READ_CODE |
| --------- | ------------------------- | --------- |
| 65363002  | Otitis media              | X00ik     |
| 16114001  | Fracture of ankle         | S34..     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 43878008  | Streptococcal sore throat | A340.     |

### Subsumption testing

Subsumption test is a fancy way of saying "is this code equal or a subtype of
this other code".

For example, a code representing "ankle fracture" is subsumed by another code
representing "fracture". The "fracture" code is more general, and using it with
subsumption can help us find other codes representing different subtypes of
fracture.

The `subsumes` function allows us to perform subsumption testing on codes within
our data. The order of the left and right operands can be reversed to query
whether a code is "subsumed by" another code.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, Coding, to_snomed_coding, subsumes

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

# 232208008 |Ear, nose and throat disorder|
left_coding = Coding('http://snomed.info/sct', '232208008')
right_coding_column = to_snomed_coding(csv.CODE)

csv.select(
        'CODE', 'DESCRIPTION',
        subsumes(left_coding, right_coding_column).alias('SUBSUMES')
).show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
csv <- pathling_spark(pc) %>%
        spark_read_csv(path = '/Users/gri306/Library/CloudStorage/OneDrive-CSIRO/Data/synthea/10k_csv_20210818/csv/conditions.csv', header = TRUE)

csv %>%
        mutate(
                CODE,
                DESCRIPTION,
                # 232208008 |Ear, nose and throat disorder|
                SUBSUMES = !!tx_subsumes(!!tx_to_snomed_coding("232208008"), !!tx_to_snomed_coding(CODE)),
                .keep = "none"
        ) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.sql.Terminology._
import au.csiro.pathling.library.TerminologyHelpers._
import au.csiro.pathling.fhirpath.encoding.CodingSchema

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

csv.select(
    csv.col("CODE"),
    // 232208008 |Ear, nose and throat disorder|
    subsumes(
        CodingSchema.toStruct(
            lit(null),
            lit(SNOMED_URI),
            lit(null),
            lit("232208008"),
            lit(null),
            lit(null)
        ),
        toSnomedCoding(csv.col("CODE"))
    ).alias("IS_ENT")
).show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import static au.csiro.pathling.sql.Terminology.*;
import static au.csiro.pathling.library.TerminologyHelpers.*;

import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        csv.select(
                csv.col("CODE"),
                // 232208008 |Ear, nose and throat disorder|
                subsumes(
                        CodingEncoding.toStruct(
                                lit(null),
                                lit(SNOMED_URI),
                                lit(null),
                                lit("232208008"),
                                lit(null),
                                lit(null)
                        ),
                        toSnomedCoding(csv.col("CODE"))
                ).alias("IS_ENT")
        ).show();
    }
}

```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION       | IS_ENT |
| --------- | ----------------- | ------ |
| 65363002  | Otitis media      | true   |
| 16114001  | Fracture of ankle | false  |
| 444814009 | Viral sinusitis   | true   |

### Retrieving properties

Some terminologies contain additional properties that are associated with codes.
You can query these properties using the `property_of` function.

There is also a `display` function that can be used to retrieve the preferred
display term for each code.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, to_snomed_coding, property_of, display, PropertyType

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

# Get the parent codes for each code in the dataset.
parents = csv.withColumn(
        "PARENTS",
        property_of(to_snomed_coding(csv.CODE), "parent", PropertyType.CODE),
)
# Split each parent code into a separate row.
exploded_parents = parents.selectExpr(
        "CODE", "DESCRIPTION", "explode_outer(PARENTS) AS PARENT"
)
# Retrieve the preferred term for each parent code.
with_displays = exploded_parents.withColumn(
        "PARENT_DISPLAY", display(to_snomed_coding(exploded_parents.PARENT))
)
with_displays.show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
csv <- pathling_spark(pc) %>%
        spark_read_csv(path = 'conditions.csv', header = TRUE)

parents <- csv %>%
        # Get the parent codes for each code in the dataset. Split each parent code into a separate row.
        mutate(
                PARENT = explode_outer(!!tx_property_of(!!tx_to_snomed_coding(CODE), "parent", "code"))
        ) %>%
        # Retrieve the preferred term for each parent code.
        mutate(
                PARENT = !!tx_display(!!tx_to_snomed_coding(PARENT))
        ) %>%
        select(CODE, DESCRIPTION, PARENT) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.sql.Terminology
import au.csiro.pathling.sql.Terminology._
import au.csiro.pathling.library.TerminologyHelpers._
import au.csiro.pathling.fhirpath.encoding.CodingSchema

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

// Get the parent codes for each code in the dataset.
val parents = csv.withColumn(
    "PARENTS",
    property_of(toSnomedCoding(csv.col("CODE")), "parent", "code")
)
// Split each parent code into a separate row.
val exploded_parents = parents.selectExpr(
    "CODE", "DESCRIPTION", "explode_outer(PARENTS) AS PARENT"
)
// Retrieve the preferred term for each parent code.
val with_displays = exploded_parents.withColumn(
    "PARENT_DISPLAY", Terminology.display(toSnomedCoding(exploded_parents.col("PARENT")))
)
with_displays.show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import static au.csiro.pathling.sql.Terminology.*;
import static au.csiro.pathling.library.TerminologyHelpers.*;

import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        // Get the parent codes for each code in the dataset.
        Dataset<Row> parents = csv.withColumn(
                "PARENTS",
                property_of(toSnomedCoding(csv.col("CODE")), "parent", "code")
        );
        // Split each parent code into a separate row.
        Dataset<Row> exploded_parents = parents.selectExpr(
                "CODE", "DESCRIPTION", "explode_outer(PARENTS) AS PARENT"
        );
        // Retrieve the preferred term for each parent code.
        Dataset<Row> with_displays = exploded_parents.withColumn(
                "PARENT_DISPLAY", Terminology.display(
                        toSnomedCoding(exploded_parents.col("PARENT")))
        );
        with_displays.show();
    }
}

```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION       | PARENT    | PARENT_DISPLAY                          |
| --------- | ----------------- | --------- | --------------------------------------- |
| 65363002  | Otitis media      | 43275000  | Otitis                                  |
| 65363002  | Otitis media      | 68996008  | Disorder of middle ear                  |
| 16114001  | Fracture of ankle | 125603006 | Injury of ankle                         |
| 16114001  | Fracture of ankle | 46866001  | Fracture of lower limb                  |
| 444814009 | Viral sinusitis   | 36971009  | Sinusitis                               |
| 444814009 | Viral sinusitis   | 281794004 | Viral upper respiratory tract infection |
| 444814009 | Viral sinusitis   | 363166002 | Infective disorder of head              |
| 444814009 | Viral sinusitis   | 36971009  | Sinusitis                               |
| 444814009 | Viral sinusitis   | 281794004 | Viral upper respiratory tract infection |
| 444814009 | Viral sinusitis   | 363166002 | Infective disorder of head              |

### Retrieving designations

Some terminologies contain additional display terms for codes. These can be used
for language translations, synonyms, and more. You can query these terms using
the `designation` function.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, to_snomed_coding, Coding, designation

pc = PathlingContext.create()
csv = pc.spark.read.csv("conditions.csv")

# Get the synonyms for each code in the dataset.
synonyms = csv.withColumn(
        "SYNONYMS",
        designation(to_snomed_coding(csv.CODE),
                    Coding.of_snomed("900000000000013009")),
)
# Split each synonyms into a separate row.
exploded_synonyms = synonyms.selectExpr(
        "CODE", "DESCRIPTION", "explode_outer(SYNONYMS) AS SYNONYM"
)
exploded_synonyms.show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
csv <- pathling_spark(pc) %>%
        spark_read_csv(path = 'conditions.csv', header = TRUE)

synonyms <- csv %>%
        # Get the synonyms for each code in the dataset.
        mutate(
                SYNONYMS = !!tx_designation(!!tx_to_snomed_coding(CODE),
                                             !!tx_to_snomed_coding("900000000000013009"))
        ) %>%
        # Split each synonym into a separate row.
        mutate(SYNONYM = explode_outer(SYNONYMS)) %>%
        select(CODE, DESCRIPTION, SYNONYM) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.sql.Terminology._
import au.csiro.pathling.library.TerminologyHelpers._
import org.hl7.fhir.r4.model.Coding

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

// Get the synonyms for each code in the dataset.
val synonyms = csv.withColumn(
    "SYNONYMS",
    designation(toSnomedCoding(csv.col("CODE")),
        new Coding("http://snomed.info/sct", "900000000000013009", null))
)
// Split each synonym into a separate row.
val exploded_synonyms = synonyms.selectExpr(
    "CODE", "DESCRIPTION", "explode_outer(SYNONYMS) AS SYNONYM"
)
exploded_synonyms.show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import static au.csiro.pathling.sql.Terminology.*;
import static au.csiro.pathling.library.TerminologyHelpers.*;

import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        Dataset<Row> csv = pc.getSpark().read().csv("conditions.csv");

        // Get the synonyms for each code in the dataset.
        Dataset<Row> synonyms = csv.withColumn(
                "SYNONYMS",
                designation(toSnomedCoding(csv.col("CODE")),
                        new Coding("http://snomed.info/sct",
                                "900000000000013009", null))
        );
        // Split each synonym into a separate row.
        Dataset<Row> exploded_synonyms = synonyms.selectExpr(
                "CODE", "DESCRIPTION", "explode_outer(SYNONYMS) AS SYNONYM"
        );
        exploded_synonyms.show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| CODE      | DESCRIPTION                          | SYNONYM                                    |
| --------- | ------------------------------------ | ------------------------------------------ |
| 65363002  | Otitis media                         | OM - Otitis media                          |
| 16114001  | Fracture of ankle                    | Ankle fracture                             |
| 16114001  | Fracture of ankle                    | Fracture of distal end of tibia and fibula |
| 444814009 | Viral sinusitis (disorder)           | NULL                                       |
| 444814009 | Viral sinusitis (disorder)           | NULL                                       |
| 43878008  | Streptococcal sore throat (disorder) | Septic sore throat                         |
| 43878008  | Streptococcal sore throat (disorder) | Strep throat                               |
| 43878008  | Streptococcal sore throat (disorder) | Strept throat                              |
| 43878008  | Streptococcal sore throat (disorder) | Streptococcal angina                       |
| 43878008  | Streptococcal sore throat (disorder) | Streptococcal pharyngitis                  |

### Multi-language support

The library enables communication of a preferred language to the terminology
server using the `Accept-Language` HTTP header, as described
in [Multi-language support in FHIR](https://hl7.org/fhir/R4/languages.html#http).
The header may contain multiple languages, with weighted preferences as defined
in [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-accept-language).
The server can use the header to return the result in the preferred language if
it is able. The actual behaviour may depend on the server implementation and the
code systems used.

The default value for the header can be configured during the creation of
the `PathlingContext` with the `accept_language` or `acceptLanguage` parameter.
The parameter with the same name can also be used to override the default value
in `display()` and `property_of()` functions.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext, to_loinc_coding, property_of, display

# Configure the default language preferences to prioritise French.
pc = PathlingContext.create(accept_language="fr;q=0.9,en;q=0.5")
csv = pc.spark.read.csv("observations.csv")

# Get the display names with default language preferences (in French).
def_display = csv.withColumn(
        "DISPLAY", display(to_loinc_coding(csv.CODE))
)

# Get the `display` property values with German as the preferred language.
def_and_german_display = def_display.withColumn(
        "DISPLAY_DE",
        property_of(to_loinc_coding(csv.CODE), "display",
                    accept_language="de-DE"),
)
def_and_german_display.show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

# Configure the default language preferences to prioritise French.
pc <- pathling_connect(accept_language = "fr;q=0.9,en;q=0.5")
csv <- pathling_spark(pc) %>%
        spark_read_csv(path = "observations.csv", header = TRUE)

csv %>%
        # Get the display names with default language preferences (in French).
        mutate(
                DISPLAY = !!tx_display(!!tx_to_loinc_coding(CODE))
        ) %>%
        # Get the `display` property values with German as the preferred language.
        mutate(
                DISPLAY_DE = explode_outer(!!tx_property_of(!!tx_to_loinc_coding(CODE), "display", "string", accept_language = "de-DE"))
        ) %>%
        select(CODE, DESCRIPTION, DISPLAY, DISPLAY_DE) %>%
        show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.sql.Terminology
import au.csiro.pathling.sql.Terminology._
import au.csiro.pathling.library.TerminologyHelpers._

// Configure the default language preferences to prioritise French.
val pc = PathlingContext.create(
    TerminologyConfiguration.builder()
            .acceptLangage("fr;q=0.9,en;q=0.5").build()
);
val csv = spark.read.csv("observations.csv")

// Get the display names with default language preferences (in French).
val defDisplay = csv.withColumn(
    "DISPLAY",
    display(toLoincCoding(csv.col("CODE")))
)
// Get the `display` property values with German as the preferred language.
val defAndGermanDisplay = defDisplay.withColumn(
    "DISPLAY_DE", property_of(toLoincCoding(csv.col("CODE")), "display", "string", "de-DE")
)
defAndGermanDisplay.show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import static au.csiro.pathling.sql.Terminology.*;
import static au.csiro.pathling.library.TerminologyHelpers.*;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.config.TerminologyConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MyApp {

    public static void main(String[] args) {
        // Configure the default language preferences to prioritise French.
        PathlingContext pc = PathlingContext.create(
                TerminologyConfiguration.builder()
                        .acceptLangage("fr;q=0.9,en;q=0.5").build()
        );
        Dataset<Row> csv = pc.getSpark().read().csv("observations.csv");

        // Get the display names with default language preferences (in French).
        Dataset<Row> defDisplay = csv.withColumn(
                "DISPLAY",
                display(toLoincCoding(csv.col("CODE")))
        );

        // Get the `display` property values with German as the preferred language.
        Dataset<Row> defAndGermanDisplay = defDisplay.withColumn(
                "DISPLAY_DE",
                property_of(toLoincCoding(csv.col("CODE")), "display", "string",
                        "de-DE")
        );
        defAndGermanDisplay.show();
    }
}
```

</TabItem>
</Tabs>

Results in:

| CODE    | DESCRIPTION                        | DISPLAY                                           | DISPLAY_DE                          |
| ------- | ---------------------------------- | ------------------------------------------------- | ----------------------------------- |
| 8302-2  | Body Height                        | Taille du patient \[Longueur] Patient ; Numérique | Körpergröße                         |
| 29463-7 | Body Weight                        | Poids corporel \[Masse] Patient ; Numérique       | Körpergewicht                       |
| 718-7   | Hemoglobin \[Mass/volume] in Blood | Hémoglobine \[Masse/Volume] Sang ; Numérique      | Hämoglobin \[Masse/Volumen] in Blut |

### Authentication

Pathling can be configured to connect to a protected terminology server by
supplying a set of OAuth2 client credentials and a token endpoint.

Here is an example of how to authenticate to
the [NHS terminology server](https://ontology.nhs.uk/):

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

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
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect(
        terminology_server_url = "https://ontology.nhs.uk/production1/fhir",
        token_endpoint = "https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token",
        client_id = "[client ID]",
        client_secret = "[client secret]"
)
```

</TabItem>
<TabItem value="scala" label="Scala">

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

```java
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.PathlingContextConfiguration;

class MyApp {

    public static void main(String[] args) {
        PathlingContextConfiguration config = PathlingContextConfiguration.builder()
                .terminologyServerUrl(
                        "https://ontology.nhs.uk/production1/fhir")
                .tokenEndpoint(
                        "https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token")
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

## Local terminology mode

By default the terminology functions call a remote FHIR terminology server. As
an alternative, Pathling can evaluate the same functions against a **local
terminology store** with no network dependency. You import SNOMED CT and FHIR
terminology content into the store once, then configure a context for local
mode pointing at that store. All seven terminology functions (`member_of`,
`translate`, `subsumes`, `subsumed_by`, `display`, `property_of` and
`designation`) work identically in local mode.

Local mode is useful when a terminology server is unavailable, when network
access or request volume is a constraint, or when reproducibility across
environments matters.

### Importing content

SNOMED CT is imported from an RF2 snapshot release (a `.zip` archive or an
extracted directory). FHIR CodeSystem, ValueSet and ConceptMap resources are
imported from a JSON file, a directory of JSON files, or a FHIR NPM package
(`.tgz`). The store is written as Delta tables under a location on any
filesystem accessible through the Hadoop FileSystem API, and can be reused
across sessions and from cluster deployments. Re-importing a version replaces it
atomically.

<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
pc.import_snomed(
    "/data/SnomedCT_InternationalRF2_PRODUCTION_20250601T120000Z.zip",
    "/data/tx-store",
)
pc.import_fhir_terminology("/data/hl7.terminology.r4-6.5.0.tgz", "/data/tx-store")
```

</TabItem>
<TabItem value="r" label="R">

```r
pc <- pathling_connect()
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store")
pathling_import_fhir_terminology(pc, "/data/hl7.terminology.tgz", "/data/tx-store")
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling import-snomed /data/rf2.zip /data/tx-store
pathling import-fhir-terminology /data/hl7.terminology.tgz /data/tx-store
```

</TabItem>
</Tabs>

#### Reducing the memory the hierarchy takes at query time

The largest structure a local store loads into memory is the hierarchy index,
which holds the transitive closure of the is-a graph as compressed bitmaps
addressed by an internal identifier per concept. By default those identifiers are
assigned in concept code order, and a code's numeric value bears no relation to
the concept's place in the hierarchy, so a concept's descendants scatter across
the whole identifier range and compress poorly.

The `pre-order` setting instead assigns identifiers by a depth-first traversal of
the is-a hierarchy, so each subtree occupies a near-contiguous interval. Measured
over a full SNOMED CT UK edition of 1,115,237 concepts, this reduces the
hierarchy index from 738 MB to 536 MB of retained heap, a saving of 27%.

The trade-off is identifier stability. Under the default ordering a concept keeps
its identifier across re-imports of any release that contains it, and identifiers
change only where codes are added or removed. Under the pre-order, a change
anywhere in the shape of the hierarchy shifts the identifiers of everything that
follows it, so identifiers vary much more between releases. Identifiers are
internal to a store and never appear in query results, so this affects nothing a
user can observe directly; it matters only if you compare or reuse the internal
identifiers of two separately imported stores. Repeated imports of the same
release remain reproducible under both orderings, and all seven terminology
functions return identical results either way.

<Tabs>
<TabItem value="python" label="Python">

```python
pc.import_snomed(
    "/data/rf2.zip",
    "/data/tx-store",
    dense_id_order="pre-order",
)
```

</TabItem>
<TabItem value="r" label="R">

```r
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store",
  dense_id_order = "pre-order")
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling import-snomed /data/rf2.zip /data/tx-store --dense-id-order pre-order
```

</TabItem>
</Tabs>

#### Large CodeSystems

CodeSystems are imported with bounded memory regardless of their size. The
import streams each CodeSystem from the source, transcodes it to temporary files
on the driver, and loads it with Spark, so peak memory does not grow with the
number of concepts and a single CodeSystem may exceed the 2 GB limit on a single
in-memory object (for example, the OMOP vocabulary package's multi-gigabyte
CodeSystem). This applies equally to a bare JSON file, a directory, and a `.tgz`
package.

Peak memory is bounded, but the largest vocabularies still need more than the
default 1 GB driver heap to hold the working set of the Spark joins that build
the store; the OMOP vocabulary, for example, imports comfortably with a 4 GB
heap. See
[driver memory for large imports](cli#terminology-import-commands) in the CLI
guide for how to raise it, which applies equally to imports run through the
library.

During a long import, a running count of parsed concepts is logged at a fixed
interval alongside stage-transition messages, so progress is visible rather than
appearing to hang. The CLI surfaces these messages in `--verbose` mode.

#### Hierarchies from parent and child properties

Many flat CodeSystems express their hierarchy through `parent` (or `child`)
concept properties rather than nested concepts. The import derives is-a edges
from code-valued `parent` and `child` properties, recognised by the standard
`parent`/`child` property codes or a property declaration carrying the standard
[concept-properties](https://hl7.org/fhir/codesystem-concept-properties.html)
URI, in addition to concept nesting. Edges from both sources are combined, so
subsumption and descendant-based membership queries work over property-based
hierarchies just as they do over nested ones. A `parent` or `child` reference to
a code absent from the CodeSystem is skipped with a warning, and duplicate
concept codes resolve to their first occurrence with a warning.

#### Bundles and non-CodeSystem resources

ValueSets and ConceptMaps are stored whole, so a single resource must fit in
memory; one larger than 1 GB fails with an actionable error naming the resource
rather than an opaque memory error. Bundle-wrapped sources are also parsed in
memory, so a Bundle is subject to the same in-memory limit; supply large
CodeSystems as standalone resources to benefit from the streaming path.

#### Recovering from a failed import

If an import fails partway through writing a CodeSystem (for example, because the
source is truncated or corrupt), it reports that the store may hold a partial
version of that CodeSystem and advises re-running the import. Because content is
keyed by system version, re-running with a corrected source fully replaces the
partial version and repairs the store.

### Querying in local mode

Create a context configured for local mode by setting the terminology mode to
`local` and pointing at the store. The terminology functions then evaluate
against the store.

<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext
from pathling.functions import to_snomed_coding
from pathling.udfs import member_of
from pyspark.sql import functions as F

pc = PathlingContext.create(
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
)

result = df.select(
    "id",
    member_of(
        to_snomed_coding(F.col("code")),
        "http://snomed.info/sct?fhir_vs=ecl/<< 73211009 |Diabetes mellitus|",
    ).alias("is_diabetes"),
)
```

</TabItem>
<TabItem value="r" label="R">

```r
pc <- pathling_connect(
  terminology_mode = "local",
  terminology_storage_path = "/data/tx-store"
)
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling --tx-store /data/tx-store member-of codes.csv \
  --code-column code --system 'http://snomed.info/sct' \
  --value-set 'http://snomed.info/sct?fhir_vs=ecl/<< 73211009'
```

The store can also be recorded once in the `[tx-store]` config table. See the
[command line interface documentation](cli#local-terminology-mode) for details.

</TabItem>
</Tabs>

The following configuration parameters control local mode:

- `terminology_mode` (`terminology.mode`): `server` (the default) or `local`.
- `terminology_storage_path` (`terminology.local.storagePath`): the store
  location, required in local mode.
- `default_snomed_edition` (`terminology.local.defaultSnomedEdition`): the
  SNOMED CT module identifier used to disambiguate an unversioned SNOMED
  reference when the store holds multiple editions.
- `expansion_cache_size` (`terminology.local.expansionCacheSize`): the maximum
  number of value set expansions cached per executor.

### Supported expressions

Local `member_of` resolves explicit imported value sets by canonical URL (with
an optional `|version`), the SNOMED implicit value set forms (`?fhir_vs`,
`?fhir_vs=refset/[id]`, `?fhir_vs=isa/[id]`, `?fhir_vs=ecl/[expr]`), and VCL
implicit value sets (`http://fhir.org/VCL?v1=[expr]`). A supported subset of
SNOMED CT Expression Constraint Language is translated to the internal VCL
model; ECL constructs outside that subset (role groups, cardinality, term
filters, history supplements and concrete values) raise an error naming the
unsupported construct. Local `translate` resolves imported ConceptMaps and the
SNOMED implicit concept map form (`?fhir_cm=[refsetId]`). Content that has not
been imported yields the same "unknown content" results as remote mode.
