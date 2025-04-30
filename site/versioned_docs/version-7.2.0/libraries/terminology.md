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
|-----------|---------------------------|-----------------|
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
|-----------|---------------------------|-----------|
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
import au.csiro.pathling.fhirpath.encoding.CodingEncoding

val pc = PathlingContext.create()
val csv = spark.read.csv("conditions.csv")

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
|-----------|-------------------|--------|
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
import au.csiro.pathling.fhirpath.encoding.CodingEncoding

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
|-----------|-------------------|-----------|-----------------------------------------|
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
|-----------|--------------------------------------|--------------------------------------------|
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

| CODE     | DESCRIPTION	                       | DISPLAY                                           | DISPLAY_DE                          
|----------|------------------------------------|---------------------------------------------------|-------------------------------------|
| 8302-2	  | Body Height	                       | Taille du patient \[Longueur] Patient ; Numérique | Körpergröße                         
| 29463-7	 | Body Weight	                       | Poids corporel \[Masse] Patient ; Numérique       | Körpergewicht                       
| 718-7	   | Hemoglobin \[Mass/volume] in Blood | Hémoglobine \[Masse/Volume] Sang ; Numérique      | Hämoglobin \[Masse/Volumen] in Blut 

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
