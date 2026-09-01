# Terminology functions

The library also provides a set of functions for querying a FHIR terminology server from within your queries and transformations.

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

The library enables communication of a preferred language to the terminology server using the `Accept-Language` HTTP header, as described in [Multi-language support in FHIR](https://hl7.org/fhir/R4/languages.html#http). The header may contain multiple languages, with weighted preferences as defined in [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-accept-language). The server can use the header to return the result in the preferred language if it is able. The actual behaviour may depend on the server implementation and the code systems used.

In local terminology mode the same value is read by Pathling itself rather than sent anywhere: weighted preferences are honoured, and each language is tried in turn until one yields a term. How a language selects a term there is described under [dialects](#dialects).

The default value for the header can be configured during the creation of the `PathlingContext` with the `accept_language` or `acceptLanguage` parameter. The parameter with the same name can also be used to override the default value in `display()` and `property_of()` functions.

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

Results in:

| CODE    | DESCRIPTION                        | DISPLAY                                           | DISPLAY\_DE                         |
| ------- | ---------------------------------- | ------------------------------------------------- | ----------------------------------- |
| 8302-2  | Body Height                        | Taille du patient \[Longueur] Patient ; Numérique | Körpergröße                         |
| 29463-7 | Body Weight                        | Poids corporel \[Masse] Patient ; Numérique       | Körpergewicht                       |
| 718-7   | Hemoglobin \[Mass/volume] in Blood | Hémoglobine \[Masse/Volume] Sang ; Numérique      | Hämoglobin \[Masse/Volumen] in Blut |

### Authentication[​](#authentication "Direct link to Authentication")

Pathling can be configured to connect to a protected terminology server by supplying a set of OAuth2 client credentials and a token endpoint.

Here is an example of how to authenticate to the [NHS terminology server](https://ontology.nhs.uk/):

* Python
* R
* Scala
* Java

```
from pathling import PathlingContext

pc = PathlingContext.create(
        terminology_server_url='https://ontology.nhs.uk/production1/fhir',
        token_endpoint='https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token',
        client_id='[client ID]',
        client_secret='[client secret]'
)
```

```
library(sparklyr)
library(pathling)

pc <- pathling_connect(
        terminology_server_url = "https://ontology.nhs.uk/production1/fhir",
        token_endpoint = "https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token",
        client_id = "[client ID]",
        client_secret = "[client secret]"
)
```

```
import au.csiro.pathling.library.{PathlingContext, PathlingContextConfiguration}

val config = PathlingContextConfiguration.builder()
        .terminologyServerUrl("https://ontology.nhs.uk/production1/fhir")
        .tokenEndpoint("https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token")
        .clientId("[client ID]")
        .clientSecret("[client secret]")
        .build()
val pc = PathlingContext.create(config)
```

```
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

## Local terminology mode[​](#local-terminology-mode "Direct link to Local terminology mode")

By default the terminology functions call a remote FHIR terminology server. As an alternative, Pathling can evaluate the same functions against a **local terminology store** with no network dependency. You import SNOMED CT and FHIR terminology content into the store once, then configure a context for local mode pointing at that store. All seven terminology functions (`member_of`, `translate`, `subsumes`, `subsumed_by`, `display`, `property_of` and `designation`) work identically in local mode.

Local mode is useful when a terminology server is unavailable, when network access or request volume is a constraint, or when reproducibility across environments matters.

### Importing content[​](#importing-content "Direct link to Importing content")

SNOMED CT is imported from an RF2 snapshot release (a `.zip` archive or an extracted directory). FHIR CodeSystem, ValueSet and ConceptMap resources are imported from a JSON file, a directory of JSON files, or a FHIR NPM package (`.tgz`). The store is written as Delta tables under a location on any filesystem accessible through the Hadoop FileSystem API, and can be reused across sessions and from cluster deployments. Re-importing a version replaces it atomically.

* Python
* R
* CLI

```
from pathling import PathlingContext

pc = PathlingContext.create()
pc.import_snomed(
    "/data/SnomedCT_InternationalRF2_PRODUCTION_20250601T120000Z.zip",
    "/data/tx-store",
)
pc.import_fhir_terminology("/data/hl7.terminology.r4-6.5.0.tgz", "/data/tx-store")
```

```
pc <- pathling_connect()
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store")
pathling_import_fhir_terminology(pc, "/data/hl7.terminology.tgz", "/data/tx-store")
```

```
pathling import-snomed /data/rf2.zip /data/tx-store
pathling import-fhir-terminology /data/hl7.terminology.tgz /data/tx-store
```

#### RF2 sources must be self-contained[​](#rf2-sources-must-be-self-contained "Direct link to RF2 sources must be self-contained")

An RF2 source is imported on its own terms: the concepts it ships are the dictionary that every other file is resolved against. A description, relationship or reference set row referencing a concept the source does not ship has nothing to attach to, so it is dropped. A relationship needs both its source and its destination concept, so one missing destination drops the row.

This is the ordinary shape of a **derived** or **extension** package. Such a package declares its dependency on another edition through the Module Dependency reference set and ships only its own modules' components, so most of what it references lives in the edition it extends. Imported alone, it succeeds while carrying almost none of the content you expected. Two published examples: the SNOMED CT International Patient Summary ships no concepts of its own at all, and the SNOMED CT Netherlands Patient Friendly Extension ships two bookkeeping concepts alongside 1,287 active descriptions, of which 7 resolve.

To tell whether this has happened, read the per-file resolution counts in the import log. Every file resolved against the concept dictionary reports one line:

```
.../sct2_Description_Snapshot-en_NL_20200930.txt: 7 of 1287 active rows resolved against the concept dictionary.
```

A line is reported for every such file, including the files that resolve completely, so a file whose two figures are equal is never confused with one that was absent. Both figures count active rows only, because rows excluded for being inactive are excluded by design rather than for want of a concept. The concept file itself, the language reference sets and the Module Dependency reference set produce no line, since none of them is resolved against the concept dictionary.

Unresolved rows are reported informationally and never fail the import: importing a package whose references are mostly external is a legitimate thing to do if that is what you intend. The lines are logged at `INFO` by the importer, so they appear wherever logging for `au.csiro.pathling` is enabled at that level.

#### Combining a derived package with its dependency[​](#combining-a-derived-package-with-its-dependency "Direct link to Combining a derived package with its dependency")

To import a derived package's content in full, combine it with the release it declares a dependency on and import the combination as a single source.

Three roles are single-valued, so their files must be **concatenated** into one file each, keeping only the first file's header row:

* the concept file (`sct2_Concept_Snapshot_*`)
* the relationship file (`sct2_Relationship_Snapshot_*`)
* the Module Dependency reference set (`der2_ssRefset_ModuleDependencySnapshot_*`)

Every other role is multi-valued, so those files are **left as they are**, each keeping its own header, in the directory layout the import expects:

* descriptions and text definitions (`sct2_Description_*`, `sct2_TextDefinition_*`)
* language reference sets (`der2_cRefset_Language*`)
* all other reference sets (`der2_*Refset*`)

```
INT=/data/SnomedCT_InternationalRF2_PRODUCTION_20250601T120000Z/Snapshot
EXT=/data/SnomedCT_ExtensionRF2_PRODUCTION_20250930T120000Z/Snapshot
OUT=/data/merged/Snapshot

mkdir -p "$OUT/Terminology" "$OUT/Refset/Language" "$OUT/Refset/Content" \
         "$OUT/Refset/Metadata"

# Single-valued roles: concatenate, keeping only the first file's header row.
concat() {
  cat "$1" > "$3"
  tail -n +2 "$2" >> "$3"
}
concat "$INT"/Terminology/sct2_Concept_Snapshot_*.txt \
       "$EXT"/Terminology/sct2_Concept_Snapshot_*.txt \
       "$OUT/Terminology/sct2_Concept_Snapshot_MERGED.txt"
concat "$INT"/Terminology/sct2_Relationship_Snapshot_*.txt \
       "$EXT"/Terminology/sct2_Relationship_Snapshot_*.txt \
       "$OUT/Terminology/sct2_Relationship_Snapshot_MERGED.txt"
concat "$INT"/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_*.txt \
       "$EXT"/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_*.txt \
       "$OUT/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_MERGED.txt"

# Multi-valued roles: copy every file as it is. The two releases name their files
# by edition and date, so nothing is overwritten.
cp "$INT"/Terminology/sct2_Description_*.txt \
   "$INT"/Terminology/sct2_TextDefinition_*.txt \
   "$EXT"/Terminology/sct2_Description_*.txt "$OUT/Terminology/"
cp "$INT"/Refset/Language/*.txt "$EXT"/Refset/Language/*.txt "$OUT/Refset/Language/"
cp "$INT"/Refset/Content/*.txt "$EXT"/Refset/Content/*.txt "$OUT/Refset/Content/"

pathling import-snomed /data/merged /data/tx-store
```

Adjust the copies for the roles a given package actually ships; an extension without text definitions, for instance, contributes none.

Do not simply extract both releases side by side into one directory. The import rejects a source in which more than one file fills a single-valued role, naming the role and every candidate path, because it cannot tell which tree's content you meant and would otherwise proceed against one and silently ignore the other.

Combine a package only with the release it declares a dependency on. An extension ships only its own modules' components, so a package and its dependency do not overlap. Two overlapping editions do: the same concept code would arrive twice, be given two internal identifiers, and fan out every join built on it. Nothing detects that, so it is yours to avoid.

#### Reducing the memory the hierarchy takes at query time[​](#reducing-the-memory-the-hierarchy-takes-at-query-time "Direct link to Reducing the memory the hierarchy takes at query time")

The largest structure a local store loads into memory is the hierarchy index, which holds the transitive closure of the is-a graph as compressed bitmaps addressed by an internal identifier per concept. By default those identifiers are assigned in concept code order, and a code's numeric value bears no relation to the concept's place in the hierarchy, so a concept's descendants scatter across the whole identifier range and compress poorly.

The `pre-order` setting instead assigns identifiers by a depth-first traversal of the is-a hierarchy, so each subtree occupies a near-contiguous interval. Measured over a full SNOMED CT UK edition of 1,115,237 concepts, this reduces the hierarchy index from 738 MB to 536 MB of retained heap, a saving of 27%.

The trade-off is identifier stability. Under the default ordering a concept keeps its identifier across re-imports of any release that contains it, and identifiers change only where codes are added or removed. Under the pre-order, a change anywhere in the shape of the hierarchy shifts the identifiers of everything that follows it, so identifiers vary much more between releases. Identifiers are internal to a store and never appear in query results, so this affects nothing a user can observe directly; it matters only if you compare or reuse the internal identifiers of two separately imported stores. Repeated imports of the same release remain reproducible under both orderings, and all seven terminology functions return identical results either way.

* Python
* R
* CLI

```
pc.import_snomed(
    "/data/rf2.zip",
    "/data/tx-store",
    dense_id_order="pre-order",
)
```

```
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store",
  dense_id_order = "pre-order")
```

```
pathling import-snomed /data/rf2.zip /data/tx-store --dense-id-order pre-order
```

#### Large CodeSystems[​](#large-codesystems "Direct link to Large CodeSystems")

CodeSystems are imported with bounded memory regardless of their size. The import streams each CodeSystem from the source, transcodes it to temporary files on the driver, and loads it with Spark, so peak memory does not grow with the number of concepts and a single CodeSystem may exceed the 2 GB limit on a single in-memory object (for example, the OMOP vocabulary package's multi-gigabyte CodeSystem). This applies equally to a bare JSON file, a directory, and a `.tgz` package.

Peak memory is bounded, but the largest vocabularies still need more than the default 1 GB driver heap to hold the working set of the Spark joins that build the store; the OMOP vocabulary, for example, imports comfortably with a 4 GB heap. See [driver memory for large imports](/docs/libraries/cli.md#terminology-import-commands) in the CLI guide for how to raise it, which applies equally to imports run through the library.

During a long import, a running count of parsed concepts is logged at a fixed interval alongside stage-transition messages, so progress is visible rather than appearing to hang. The CLI surfaces these messages in `--verbose` mode.

#### Hierarchies from parent and child properties[​](#hierarchies-from-parent-and-child-properties "Direct link to Hierarchies from parent and child properties")

Many flat CodeSystems express their hierarchy through `parent` (or `child`) concept properties rather than nested concepts. The import derives is-a edges from code-valued `parent` and `child` properties, recognised by the standard `parent`/`child` property codes or a property declaration carrying the standard [concept-properties](https://hl7.org/fhir/codesystem-concept-properties.html) URI, in addition to concept nesting. Edges from both sources are combined, so subsumption and descendant-based membership queries work over property-based hierarchies just as they do over nested ones. A `parent` or `child` reference to a code absent from the CodeSystem is skipped with a warning, and duplicate concept codes resolve to their first occurrence with a warning.

#### Bundles and non-CodeSystem resources[​](#bundles-and-non-codesystem-resources "Direct link to Bundles and non-CodeSystem resources")

ValueSets and ConceptMaps are stored whole, so a single resource must fit in memory; one larger than 1 GB fails with an actionable error naming the resource rather than an opaque memory error. Bundle-wrapped sources are also parsed in memory, so a Bundle is subject to the same in-memory limit; supply large CodeSystems as standalone resources to benefit from the streaming path.

#### Recovering from a failed import[​](#recovering-from-a-failed-import "Direct link to Recovering from a failed import")

If an import fails partway through writing a CodeSystem (for example, because the source is truncated or corrupt), it reports that the store may hold a partial version of that CodeSystem and advises re-running the import. Because content is keyed by system version, re-running with a corrected source fully replaces the partial version and repairs the store.

### Querying in local mode[​](#querying-in-local-mode "Direct link to Querying in local mode")

Create a context configured for local mode by setting the terminology mode to `local` and pointing at the store. The terminology functions then evaluate against the store.

* Python
* R
* CLI

```
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

```
pc <- pathling_connect(
  terminology_mode = "local",
  terminology_storage_path = "/data/tx-store"
)
```

```
pathling --tx-store /data/tx-store member-of codes.csv \
  --code-column code --system 'http://snomed.info/sct' \
  --value-set 'http://snomed.info/sct?fhir_vs=ecl/<< 73211009'
```

The store can also be recorded once in the `[tx-store]` config table. See the [command line interface documentation](/docs/libraries/cli.md#local-terminology-mode) for details.

The following configuration parameters control local mode:

* `terminology_mode` (`terminology.mode`): `server` (the default) or `local`.
* `terminology_storage_path` (`terminology.local.storagePath`): the store location, required in local mode.
* `default_snomed_edition` (`terminology.local.defaultSnomedEdition`): the SNOMED CT module identifier used to disambiguate an unversioned SNOMED reference when the store holds multiple editions.
* `expansion_cache_size` (`terminology.local.expansionCacheSize`): the maximum number of value set expansions cached per executor.
* `dialect_aliases` (`terminology.local.dialectAliases`): additional dialect tags recognised when a display or designation is requested in a particular language. See [dialects](#dialects).

### Dialects[​](#dialects "Direct link to Dialects")

Within SNOMED CT, which of a concept's synonyms is its *preferred term* is not a property of the concept but of a **language reference set**. Two reference sets of the same language routinely disagree: the International edition ships both GB English and US English, and `32849002` is "Oesophageal structure" in the first and "Esophageal structure" in the second. A **dialect** is the caller-facing name for one of those reference sets.

#### Naming a dialect[​](#naming-a-dialect "Direct link to Naming a dialect")

A dialect may be named in any of three ways, and all three are interchangeable:

| Form                       | Example                             | Notes                                                                                                                                       |
| -------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| A recognised tag           | `en-GB`                             | Matched without regard to case. The recognised tags are below.                                                                              |
| An extension tag           | `en-x-sctlang-90000000-00005080-04` | The form Pathling reports as the language of a preferred designation, so a language reported on the way out can be requested on the way in. |
| A reference set identifier | `900000000000508004`                | Accepted by the import option only, not by a query-time language request.                                                                   |

The following tags are recognised out of the box. They cover the language reference sets defined in the SNOMED CT International edition; a reference set defined inside a national extension is reached through an alias or through the extension tag form.

| Tag     | Language reference set |
| ------- | ---------------------- |
| `en-GB` | `900000000000508004`   |
| `en-US` | `900000000000509007`   |
| `en-AU` | `32570271000036106`    |
| `es`    | `448879004`            |
| `fr`    | `722131000`            |
| `de`    | `722130004`            |
| `ja`    | `722129009`            |
| `zh`    | `722128001`            |

A tag naming no reference set - a bare `en`, or a region nothing covers - expresses no preference rather than an error, and the stored display answers.

#### Requesting a term in a dialect[​](#requesting-a-term-in-a-dialect "Direct link to Requesting a term in a dialect")

The `accept_language` context parameter, and the parameter of the same name on `display()` and `property_of()`, select by dialect:

```
pc = PathlingContext.create(
    spark,
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
)

# "Oesophageal structure", the term GB English prefers.
british = property_of(coding, "display", accept_language="en-GB")

# "Esophageal structure", the term US English prefers.
american = property_of(coding, "display", accept_language="en-US")
```

A weighted list is read as RFC 9110 describes, and each dialect is tried in descending order of weight until one yields a term. With `accept_language="en-NZ;q=0.9,en-GB;q=0.5"` against a store holding no New Zealand reference set, the GB English term answers. A tag given zero weight is never used, and a lone `*` expresses no preference.

#### The default dialect of a store[​](#the-default-dialect-of-a-store "Direct link to The default dialect of a store")

Every concept in the store carries one **stored display**, which is what a request naming no dialect - or naming one the store cannot serve - receives. That display is the preferred synonym of a single dialect, chosen when the release is imported:

1. The dialect named by the `default_dialect` import option, if one is given.
2. The sole language reference set, where the release holds only one.
3. US English, where the release is the SNOMED CT International edition.

A release that holds several language reference sets and is not the International edition **fails the import**, listing every candidate by identifier and by the name the release itself gives it, so that one can be named:

```
The release holds 3 language reference sets and none of them is a clear default. Name one with the defaultDialect import option:
  900000000000508004  Great Britain English language reference set
  999000691000001104  National Health Service realm language reference set (pharmacy part)
  999001261000000100  NHS realm language reference set (clinical part)
```

No SNOMED CT release declares which of its language reference sets is the default, so where the release is genuinely ambiguous the choice is the operator's rather than a guess. Nothing is written to the store when the import fails this way.

* Python
* R
* CLI

```
pc.import_snomed("/data/rf2.zip", "/data/tx-store", default_dialect="en-GB")
```

```
pathling_import_snomed(pc, "/data/rf2.zip", "/data/tx-store", default_dialect = "en-GB")
```

```
pathling import-snomed --default-dialect en-GB /data/rf2.zip /data/tx-store
```

The dialect can also be recorded once as the `tx-store.default-dialect` config key, which applies whenever the flag is omitted - see the [command line interface documentation](/docs/libraries/cli.md#terminology-import-commands).

Where the chosen dialect marks no preferred synonym for a concept, its display falls to the preferred synonym of the lowest-numbered other language reference set, then to its fully specified name, and finally to its own code.

#### Registering additional dialects[​](#registering-additional-dialects "Direct link to Registering additional dialects")

A deployment can register its own dialect tags, which is how a reference set defined inside a national extension is reached by a familiar name. An entry for a tag that is already recognised replaces the built-in mapping, so a built-in entry can be corrected.

* Python
* R
* CLI

```
pc = PathlingContext.create(
    spark,
    terminology_mode="local",
    terminology_storage_path="/data/tx-store",
    dialect_aliases={"en-NZ": "271000210107"},
)
```

```
pc <- pathling_connect(
  terminology_mode = "local",
  terminology_storage_path = "/data/tx-store",
  dialect_aliases = c("en-NZ" = "271000210107")
)
```

```
[tx-store]
path = "/data/tx-store"

[tx-store.dialect-aliases]
en-NZ = "271000210107"
```

Aliases affect the selection of a display and of designations only. They are not consulted by an import, which receives no service configuration; a reference set outside the recognised tags is named there by its identifier.

The R binding can carry at most ten aliases, a limit of how sparklyr passes a map to the JVM. The Java, Python and command line surfaces have no such limit.

#### Code systems that are not SNOMED CT[​](#code-systems-that-are-not-snomed-ct "Direct link to Code systems that are not SNOMED CT")

A FHIR CodeSystem carries plain BCP-47 designation languages with no reference set to resolve, so there a language request is matched against the designation languages directly. A designation whose tag matches the request exactly is preferred over one matching only on the primary subtag, and within each of those one whose use is `display` is preferred. An extension tag has no meaning outside SNOMED CT, so it falls back to its plain language subtag.

### Supported expressions[​](#supported-expressions "Direct link to Supported expressions")

Local `member_of` resolves explicit imported value sets by canonical URL (with an optional `|version`), the SNOMED implicit value set forms (`?fhir_vs`, `?fhir_vs=refset/[id]`, `?fhir_vs=isa/[id]`, `?fhir_vs=ecl/[expr]`), and VCL implicit value sets (`http://fhir.org/VCL?v1=[expr]`). A supported subset of SNOMED CT Expression Constraint Language is translated to the internal VCL model; ECL constructs outside that subset (role groups, cardinality, term filters, history supplements and concrete values) raise an error naming the unsupported construct. Local `translate` resolves imported ConceptMaps and the SNOMED implicit concept map form (`?fhir_cm=[refsetId]`). Content that has not been imported yields the same "unknown content" results as remote mode.
