---
sidebar_position: 4
title: Adding display terms to codes
description: Tutorial demonstrating how to add human-readable display terms to medical codes using Pathling's terminology functions.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

# Adding display terms to codes

<iframe width="560" height="315" src="https://www.youtube.com/embed/5cMPfwKjNr4?si=vP6lxe45dZnxaf5P" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

Healthcare data often contains coded values that are not immediately
human-readable. For example, a SNOMED CT code like `444814009` represents "Viral
sinusitis", but without the corresponding display term, the data is difficult to
interpret. This tutorial demonstrates how to use Pathling's terminology
functions along with
a [FHIR terminology service](https://hl7.org/fhir/terminology-service.html) to
add human-readable display terms to codes in your datasets. Note that this guide
is not specific to FHIR data - you can use these techniques with any dataset
containing coded medical information.

## Why display terms matter

Codes serve as standardised identifiers for clinical concepts, but they present
several challenges:

- **Interpretation difficulty**: Codes like `403190006` or `72892002` are
  meaningless without context
- **Data analysis barriers**: Analysts need readable terms to understand
  patterns and trends
- **Reporting requirements**: Human-readable reports require display terms
  alongside codes
- **Quality assurance**: Display terms help validate that codes match intended
  concepts

Pathling's terminology functions solve these challenges by connecting to FHIR
terminology servers to retrieve official display terms, synonyms, and other
metadata for medical codes.

## Loading data with codes

For this tutorial, we'll work with a sample dataset containing medical diagnosis
codes. Let's load a CSV file with condition records:

<Tabs>
<TabItem value="python" label="Python">

```python
# Load sample data with medical condition codes
csv = pc.spark.read.options(header=True).csv("conditions.csv")

# Display the structure of our data
csv.show()
```

</TabItem>
<TabItem value="r" label="R">

```r
# Load sample data with medical condition codes
csv <- pathling_spark(pc) %>%
        spark_read_csv("conditions.csv", header = TRUE)

# Display the structure of our data
csv %>% head()
```

</TabItem>
</Tabs>

Our example dataset contains the following columns:

- `PATIENT`: Unique patient identifier
- `ENCOUNTER`: Unique encounter identifier
- `CODE`: SNOMED CT diagnosis codes
- `DESCRIPTION`: Original description of the condition
- `START`: Start date of the condition
- `STOP`: End date of the condition (if resolved)

## Adding display terms for SNOMED CT codes

The most common use case is adding display terms for SNOMED CT diagnosis codes.
The `display()` function retrieves the preferred term for each code:

<Tabs>
<TabItem value="python" label="Python">

```python
# Add SNOMED CT display terms
result = csv.withColumn("DISPLAY_NAME", display(to_snomed_coding(csv.CODE)))

# Show results
result.select("CODE", "DESCRIPTION", "DISPLAY_NAME").show(truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
# Add SNOMED CT display terms
result <- csv %>%
        mutate(
                DISPLAY_NAME = !!tx_display(!!tx_to_snomed_coding(CODE))
        )

# Show results
result %>%
        select(CODE, DESCRIPTION, DISPLAY_NAME) %>%
        head()
```

</TabItem>
</Tabs>

This produces results like:

| CODE      | DESCRIPTION                          | DISPLAY_NAME              |
|-----------|--------------------------------------|---------------------------|
| 65363002  | Otitis media                         | Otitis media              |
| 16114001  | Fracture of ankle                    | Fracture of ankle         |
| 444814009 | Viral sinusitis (disorder)           | Viral sinusitis           |
| 43878008  | Streptococcal sore throat (disorder) | Streptococcal sore throat |
| 403190006 | First degree burn                    | Epidermal burn of skin    |

Notice how the display terms often provide cleaner, more concise terminology.
For example, "(disorder)" suffixes are removed, and some codes get more precise
clinical terms like "Epidermal burn of skin" instead of "First degree burn".

## Working with different code systems

Pathling supports multiple terminology systems beyond SNOMED CT. You can work
with any code system supported by your FHIR terminology server by using the
`to_coding()` function to specify the system URI. Here's an example using
ICD-10-NL (Dutch ICD-10) codes:

<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import to_coding

# Add display terms for ICD-10-NL codes
icd_enriched = icd_data.withColumn(
    "ICD_DISPLAY",
    display(to_coding(icd_data.CODE, "http://hl7.org/fhir/sid/icd-10-nl"))
)
```

</TabItem>
<TabItem value="r" label="R">

```r
# Add display terms for ICD-10-NL codes
icd_enriched <- icd_data %>%
        mutate(
                ICD_DISPLAY = !!tx_display(!!tx_to_coding(CODE, "http://hl7.org/fhir/sid/icd-10-nl"))
        )
```

</TabItem>
</Tabs>

## Requesting display terms in different languages

Many terminology servers support multi-language display terms, allowing you to
retrieve terms in the language most appropriate for your users. Pathling
provides language support through the `accept_language` parameter, which follows
the HTTP Accept-Language header format.

<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import to_loinc_coding

# Create sample LOINC observation data
loinc_data = pc.spark.createDataFrame([
    ("8302-2", "Body Height"),
    ("29463-7", "Body Weight"),
    ("718-7", "Hemoglobin [Mass/volume] in Blood"),
    ("8867-4", "Heart rate")
], ["CODE", "DESCRIPTION"])

# Get display terms in French
loinc_french = loinc_data.withColumn(
    "DISPLAY_FR",
    display(to_loinc_coding(loinc_data.CODE), accept_language="fr")
)

# Get display terms in multiple languages
loinc_multilingual = loinc_data.withColumn(
    "DISPLAY_EN",
    display(to_loinc_coding(loinc_data.CODE))  # Default language (English)
).withColumn(
    "DISPLAY_FR",
    display(to_loinc_coding(loinc_data.CODE), accept_language="fr")
).withColumn(
    "DISPLAY_DE",
    display(to_loinc_coding(loinc_data.CODE), accept_language="de")
)

# Show the multilingual results
loinc_multilingual.select("CODE", "DISPLAY_EN", "DISPLAY_FR",
                          "DISPLAY_DE").show(truncate=False)
```

</TabItem>
<TabItem value="r" label="R">

```r
# Create sample LOINC observation data
loinc_data <- tibble(
        CODE = c("8302-2", "29463-7", "718-7", "8867-4"),
        DESCRIPTION = c(
                "Body Height",
                "Body Weight",
                "Hemoglobin [Mass/volume] in Blood",
                "Heart rate"
        )
) %>%
        copy_to(pathling_spark(pc), ., name = "loinc_data")

# Get display terms in French
loinc_french <- loinc_data %>%
        mutate(
                DISPLAY_FR = !!tx_display(!!tx_to_loinc_coding(CODE), accept_language = "fr")
        )

# Get display terms in multiple languages
loinc_multilingual <- loinc_data %>%
        mutate(
                DISPLAY_EN = !!tx_display(!!tx_to_loinc_coding(CODE)),  # Default language (English)
                DISPLAY_FR = !!tx_display(!!tx_to_loinc_coding(CODE), accept_language = "fr"),
                DISPLAY_DE = !!tx_display(!!tx_to_loinc_coding(CODE), accept_language = "de")
        )

# Show the multilingual results
loinc_multilingual %>%
        select(CODE, DISPLAY_EN, DISPLAY_FR, DISPLAY_DE) %>%
        head()
```

</TabItem>
</Tabs>

This produces multilingual display terms like:

| CODE    | DISPLAY_EN                        | DISPLAY_FR                                       | DISPLAY_DE                         |
|---------|-----------------------------------|--------------------------------------------------|------------------------------------|
| 8302-2  | Body Height                       | Taille du patient [Longueur] Patient ; Numérique | Körpergröße                        |
| 29463-7 | Body Weight                       | Poids corporel [Masse] Patient ; Numérique       | Körpergewicht                      |
| 718-7   | Hemoglobin [Mass/volume] in Blood | Hémoglobine [Masse/Volume] Sang ; Numérique      | Hämoglobin [Masse/Volumen] in Blut |
| 8867-4  | Heart rate                        | Fréquence cardiaque                              | Herzfrequenz                       |

### Language configuration options

You can configure language preferences in several ways:

1. **Set a default language for the entire session**:
   ```python
   pc = PathlingContext.create(accept_language="fr")
   ```

2. **Override language per function call**:
   ```python
   display(coding, accept_language="de")
   ```

3. **Use weighted preferences** (server will use the first available):
   ```python
   display(coding, accept_language="fr;q=0.9,en;q=0.5")
   ```

Note that language support depends on your terminology server's capabilities and
the specific code system. LOINC and SNOMED CT generally have good multi-language
support, while other systems may be more limited.

## Working with large datasets

Pathling includes an internal terminology request cache that automatically
optimises repeated code lookups. This means you can process large datasets
directly without worrying about duplicate terminology server requests.

The internal cache ensures that once a code has been looked up, subsequent
requests for the same code return immediately without contacting the terminology
server. This makes the simple, direct approach both performant and easy to
understand.

## Further reading

- [Terminology functions](/docs/libraries/terminology.md) - Complete reference
  for all terminology functions
- [Grouping and analysing SNOMED CT data](/docs/libraries/examples/grouping-snomed.md) -
  Advanced techniques for working with SNOMED CT hierarchies
- [YouTube tutorial: Adding display terms to codes](https://www.youtube.com/watch?v=5cMPfwKjNr4) -
  Video walkthrough of the techniques demonstrated in this example
- [FHIR Terminology Services](https://hl7.org/fhir/terminology-service.html) -
  Understanding the underlying terminology server capabilities
