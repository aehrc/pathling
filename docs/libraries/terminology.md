# Terminology functions

The library also provides a set of functions for querying a FHIR terminology server from within your queries and transformations.

By default the functions are evaluated by a remote FHIR terminology server. See [terminology server support](/docs/libraries/terminology/server.md) for how to configure the server, including caching and authentication. The same functions can also be evaluated against a [local terminology store](/docs/libraries/terminology/local.md) with no network dependency.

<!-- -->

### Value set membership[​](#value-set-membership "Direct link to Value set membership")

The `member_of` function can be used to test the membership of a code within a [FHIR value set](https://hl7.org/fhir/valueset.html). This can be used with both explicit value sets (i.e. those that have been pre-defined and loaded into the terminology server) and implicit value sets (e.g. SNOMED CT [Expression Constraint Language](http://snomed.org/ecl)).

In this example, we take a list of SNOMED CT diagnosis codes and create a new column which shows which are viral infections. We use an ECL expression to define viral infection as a disease with a pathological process of "Infectious process", and a causative agent of "Virus".

* Python
* R
* Scala
* Java

```
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

```
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

```
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

```
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

Results in:

| CODE      | DESCRIPTION               | VIRAL\_INFECTION |
| --------- | ------------------------- | ---------------- |
| 65363002  | Otitis media              | false            |
| 16114001  | Fracture of ankle         | false            |
| 444814009 | Viral sinusitis           | true             |
| 444814009 | Viral sinusitis           | true             |
| 43878008  | Streptococcal sore throat | false            |

### Concept translation[​](#concept-translation "Direct link to Concept translation")

The `translate` function can be used to translate codes from one code system to another using maps that are known to the terminology server. In this example, we translate our SNOMED CT diagnosis codes into [Read CTV3](https://digital.nhs.uk/services/terminology-and-classifications/read-codes).

Please note that the type of the output column is the array of coding structs, as the translation may produce multiple results for each input coding.

* Python
* R
* Scala
* Java

```
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

```
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

```
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

```
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

Results in:

| CODE      | DESCRIPTION               | READ\_CODE |
| --------- | ------------------------- | ---------- |
| 65363002  | Otitis media              | X00ik      |
| 16114001  | Fracture of ankle         | S34..      |
| 444814009 | Viral sinusitis           | XUjp0      |
| 444814009 | Viral sinusitis           | XUjp0      |
| 43878008  | Streptococcal sore throat | A340.      |

### Subsumption testing[​](#subsumption-testing "Direct link to Subsumption testing")

Subsumption test is a fancy way of saying "is this code equal or a subtype of this other code".

For example, a code representing "ankle fracture" is subsumed by another code representing "fracture". The "fracture" code is more general, and using it with subsumption can help us find other codes representing different subtypes of fracture.

The `subsumes` function allows us to perform subsumption testing on codes within our data. The order of the left and right operands can be reversed to query whether a code is "subsumed by" another code.

* Python
* R
* Scala
* Java

```
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

```
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

```
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

```
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

Results in:

| CODE      | DESCRIPTION       | IS\_ENT |
| --------- | ----------------- | ------- |
| 65363002  | Otitis media      | true    |
| 16114001  | Fracture of ankle | false   |
| 444814009 | Viral sinusitis   | true    |

### Retrieving properties[​](#retrieving-properties "Direct link to Retrieving properties")

Some terminologies contain additional properties that are associated with codes. You can query these properties using the `property_of` function.

There is also a `display` function that can be used to retrieve the preferred display term for each code.

* Python
* R
* Scala
* Java

```
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

```
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

```
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

```
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

Results in:

| CODE      | DESCRIPTION       | PARENT    | PARENT\_DISPLAY                         |
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

### Retrieving designations[​](#retrieving-designations "Direct link to Retrieving designations")

Some terminologies contain additional display terms for codes. These can be used for language translations, synonyms, and more. You can query these terms using the `designation` function.

* Python
* R
* Scala
* Java

```
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

```
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

```
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

```
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

### Multi-language support[​](#multi-language-support "Direct link to Multi-language support")

A preferred language can be set for the whole session with the `accept_language` or `acceptLanguage` parameter of `PathlingContext`, and overridden per call with the parameter of the same name on `display()` and `property_of()`. The value is an [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-accept-language) `Accept-Language` list, so it may name several languages with weighted preferences, for example `fr;q=0.9,en;q=0.5`.

The setting affects the `display` property only, whether requested through `display()` or through `property_of(coding, "display")`. It has no effect on `designation()`, `member_of()`, `subsumes()`, `translate()` or `validate_code()`.

#### Server mode[​](#server-mode "Direct link to Server mode")

The value is sent to the terminology server as the `Accept-Language` HTTP header, as described in [Multi-language support in FHIR](https://hl7.org/fhir/R4/languages.html#http). Whether and how the server honours it depends on the server implementation and the code systems it holds.

#### Local mode[​](#local-mode "Direct link to Local mode")

The value is read by Pathling itself. Each language in the list is tried in descending order of weight, and the first that yields a term answers; when none does, the display stored at import time is returned. A language the store does not recognise is not an error, it simply yields no term.

How a language selects a term depends on the code system:

* For SNOMED CT, a language tag names a language reference set, and the synonym that reference set marks as preferred is returned. `en-GB` and `en-US` are therefore distinct, and a bare `en` names nothing. The recognised tags, and how to register more, are described under [dialects](/docs/libraries/terminology/local.md#dialects).
* For a FHIR CodeSystem loaded into the store, the designation whose language matches the tag is returned. See [code systems that are not SNOMED CT](/docs/libraries/terminology/local.md#code-systems-that-are-not-snomed-ct).

`designation()` returns every designation regardless of `accept_language`. Its own `language` argument filters on the language string each designation carries, matched exactly. In local mode, a SNOMED CT synonym preferred within a language reference set carries an extension tag such as `en-x-sctlang-90000000-00005080-04` rather than `en-GB`, while acceptable synonyms and the stored display carry the plain description language, `en`.

```
from pathling import PathlingContext, to_snomed_coding, display, designation, Coding

pc = PathlingContext.create(
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
    accept_language="en-GB",
)
csv = pc.spark.read.csv("conditions.csv")

# "Oesophageal structure" for 32849002, the term GB English prefers.
british = csv.withColumn("DISPLAY", display(to_snomed_coding(csv.CODE)))

# "Esophageal structure", overriding the session default with US English.
american = british.withColumn(
    "DISPLAY_US", display(to_snomed_coding(csv.CODE), accept_language="en-US")
)

# The term preferred by GB English, requested as a designation. The language
# is the extension tag form, not "en-GB".
preferred = american.withColumn(
    "PREFERRED_GB",
    designation(
        to_snomed_coding(csv.CODE),
        Coding(
            "http://terminology.hl7.org/CodeSystem/hl7TermMaintInfra",
            "preferredForLanguage",
        ),
        "en-x-sctlang-90000000-00005080-04",
    ),
)
preferred.show()
```

#### Example[​](#example "Direct link to Example")

The following example queries LOINC in server mode. It cannot be run in local mode, which holds no LOINC content.

* Python
* R
* Scala
* Java

```
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

```
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

```
import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.sql.Terminology
import au.csiro.pathling.sql.Terminology._
import au.csiro.pathling.library.TerminologyHelpers._

// Configure the default language preferences to prioritise French.
val pc = PathlingContext.create(
    TerminologyConfiguration.builder()
            .acceptLanguage("fr;q=0.9,en;q=0.5").build()
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

```
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
                        .acceptLanguage("fr;q=0.9,en;q=0.5").build()
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

Results in:

| CODE    | DESCRIPTION                        | DISPLAY                                           | DISPLAY\_DE                         |
| ------- | ---------------------------------- | ------------------------------------------------- | ----------------------------------- |
| 8302-2  | Body Height                        | Taille du patient \[Longueur] Patient ; Numérique | Körpergröße                         |
| 29463-7 | Body Weight                        | Poids corporel \[Masse] Patient ; Numérique       | Körpergewicht                       |
| 718-7   | Hemoglobin \[Mass/volume] in Blood | Hémoglobine \[Masse/Volume] Sang ; Numérique      | Hämoglobin \[Masse/Volumen] in Blut |
