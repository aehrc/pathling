---
sidebar_position: 3
description: How to expose a FHIR value set or concept map as a table that Spark SQL can join to, following the SQL on FHIR Terminology in SQL page.
---

# Terminology in SQL

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

The library can return the content of a FHIR value set or concept map as a
Spark DataFrame, so that a SQL query can join to it like any other table. The
DataFrames follow the relations defined by the SQL on FHIR
[Terminology in SQL](https://build.fhir.org/ig/HL7/sql-on-fhir/en/terminology-in-sql.html)
page, so a query written against that specification runs unchanged once each
relation is registered under the label the query uses.

The content is obtained from the configured terminology layer - a
[terminology server](./server.md) or the [local terminology store](./local.md) -
when the DataFrame is created, and held in the DataFrame. Every use of the
DataFrame therefore sees the same content, however many times it is evaluated.

## Value sets

A value set is identified by its canonical URL, optionally followed by a
`|version` suffix. The DataFrame has one row per member of the value set:

| Column     | Type    | Null | Content                                                       |
| ---------- | ------- | ---- | ------------------------------------------------------------- |
| `system`   | string  | No   | The code system of the member                                 |
| `version`  | string  | Yes  | The code system version used to determine membership          |
| `code`     | string  | No   | The member code                                               |
| `display`  | string  | Yes  | The display text for the code                                 |
| `inactive` | boolean | Yes  | `true` where the code is inactive; null for most active codes |

In this example, the conditions whose code belongs to a value set are selected
with a semi-join, which tests membership without multiplying rows where the
value set holds a code under more than one code system version.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()
pc.spark.read.parquet("conditions.parquet").createOrReplaceTempView("conditions")
pc.value_set(
    "http://example.org/ValueSet/cardiovascular-disease|2026"
).createOrReplaceTempView("cvd_codes")

pc.spark.sql("""
    SELECT conditions.patient_id, conditions.code
    FROM conditions
    WHERE EXISTS (
      SELECT 1
      FROM cvd_codes
      WHERE cvd_codes.system = conditions.system
        AND cvd_codes.code = conditions.code
    )
""").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()
spark <- pathling_spark(pc)
spark_read_parquet(spark, "conditions", "conditions.parquet")
cvd_codes <- pathling_value_set(
  pc, "http://example.org/ValueSet/cardiovascular-disease|2026"
)
sdf_register(cvd_codes, "cvd_codes")

sdf_sql(spark, "
    SELECT conditions.patient_id, conditions.code
    FROM conditions
    WHERE EXISTS (
      SELECT 1
      FROM cvd_codes
      WHERE cvd_codes.system = conditions.system
        AND cvd_codes.code = conditions.code
    )
")

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val pc = PathlingContext.create()
pc.getSpark.read.parquet("conditions.parquet").createOrReplaceTempView("conditions")
pc.valueSet("http://example.org/ValueSet/cardiovascular-disease|2026")
    .createOrReplaceTempView("cvd_codes")

pc.getSpark.sql("""
    SELECT conditions.patient_id, conditions.code
    FROM conditions
    WHERE EXISTS (
      SELECT 1
      FROM cvd_codes
      WHERE cvd_codes.system = conditions.system
        AND cvd_codes.code = conditions.code
    )
""").show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;

class MyApp {

    public static void main(String[] args) {
        PathlingContext pc = PathlingContext.create();
        pc.getSpark().read().parquet("conditions.parquet")
                .createOrReplaceTempView("conditions");
        pc.valueSet("http://example.org/ValueSet/cardiovascular-disease|2026")
                .createOrReplaceTempView("cvd_codes");

        pc.getSpark().sql("""
                SELECT conditions.patient_id, conditions.code
                FROM conditions
                WHERE EXISTS (
                  SELECT 1
                  FROM cvd_codes
                  WHERE cvd_codes.system = conditions.system
                    AND cvd_codes.code = conditions.code
                )
                """).show();
    }

}
```

</TabItem>
</Tabs>

An implicit value set, such as a SNOMED CT ECL expression, can be used in the
same way. Build its URL with `to_ecl_value_set` (`toEclValueSet` in Java and
Scala, `tx_to_ecl_value_set` in R), which encodes the expression. A raw ECL
expression containing `|` characters would otherwise be read as a version
suffix, because the canonical is split at its first `|`.

## Concept maps

A concept map is identified in the same way. The DataFrame has one row per
mapping, plus one row for each source code that the map states has no mapping:

| Column           | Type   | Null | Content                                                                                                                      |
| ---------------- | ------ | ---- | ---------------------------------------------------------------------------------------------------------------------------- |
| `source_system`  | string | No   | The code system of the source code                                                                                           |
| `source_version` | string | Yes  | The source code system version the mapping was authored against                                                              |
| `source_code`    | string | No   | The source code                                                                                                              |
| `source_display` | string | Yes  | The display text for the source code                                                                                         |
| `target_system`  | string | Yes  | The code system the mapping translates into                                                                                  |
| `target_version` | string | Yes  | The target code system version                                                                                               |
| `target_code`    | string | Yes  | The target code; null where the map states there is no mapping                                                               |
| `target_display` | string | Yes  | The display text for the target code                                                                                         |
| `relationship`   | string | Yes  | A [ConceptMapRelationship](https://hl7.org/fhir/valueset-concept-map-relationship.html) code; null where there is no mapping |

The `relationship` is an R5 code. The equivalence of an R4 ConceptMap is
converted to it, so that `narrower` becomes `source-is-broader-than-target`, for
example. The `|version` suffix of a group's `source` and `target` is split into
the version columns. Mappings that a flat table cannot represent, such as
targets that carry `dependsOn` or `product`, cause the concept map to be
rejected rather than partially returned. The `group.unmapped` default is not
applied; write `COALESCE(sct_to_icd10.target_code, conditions.code)` to pass
unmapped codes through.

A `LEFT JOIN` translates each code and keeps the codes that the map does not
translate. The `relationship` test belongs in the `ON` clause, and admits null
so that rows stating there is no mapping are kept:

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
pc.concept_map(
    "http://example.org/ConceptMap/sct-to-icd10|2026"
).createOrReplaceTempView("sct_to_icd10")

pc.spark.sql("""
    SELECT conditions.patient_id,
           conditions.code,
           sct_to_icd10.target_code,
           sct_to_icd10.relationship
    FROM conditions
    LEFT JOIN sct_to_icd10
      ON sct_to_icd10.source_system = conditions.system
     AND sct_to_icd10.source_code = conditions.code
     AND (sct_to_icd10.relationship IS NULL
          OR sct_to_icd10.relationship <> 'not-related-to')
""").show()
```

</TabItem>
<TabItem value="r" label="R">

```r
sct_to_icd10 <- pathling_concept_map(
  pc, "http://example.org/ConceptMap/sct-to-icd10|2026"
)
sdf_register(sct_to_icd10, "sct_to_icd10")

sdf_sql(spark, "
    SELECT conditions.patient_id,
           conditions.code,
           sct_to_icd10.target_code,
           sct_to_icd10.relationship
    FROM conditions
    LEFT JOIN sct_to_icd10
      ON sct_to_icd10.source_system = conditions.system
     AND sct_to_icd10.source_code = conditions.code
     AND (sct_to_icd10.relationship IS NULL
          OR sct_to_icd10.relationship <> 'not-related-to')
")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
pc.conceptMap("http://example.org/ConceptMap/sct-to-icd10|2026")
    .createOrReplaceTempView("sct_to_icd10")

pc.getSpark.sql("""
    SELECT conditions.patient_id,
           conditions.code,
           sct_to_icd10.target_code,
           sct_to_icd10.relationship
    FROM conditions
    LEFT JOIN sct_to_icd10
      ON sct_to_icd10.source_system = conditions.system
     AND sct_to_icd10.source_code = conditions.code
     AND (sct_to_icd10.relationship IS NULL
          OR sct_to_icd10.relationship <> 'not-related-to')
""").show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
pc.conceptMap("http://example.org/ConceptMap/sct-to-icd10|2026")
        .createOrReplaceTempView("sct_to_icd10");

pc.getSpark().sql("""
        SELECT conditions.patient_id,
               conditions.code,
               sct_to_icd10.target_code,
               sct_to_icd10.relationship
        FROM conditions
        LEFT JOIN sct_to_icd10
          ON sct_to_icd10.source_system = conditions.system
         AND sct_to_icd10.source_code = conditions.code
         AND (sct_to_icd10.relationship IS NULL
              OR sct_to_icd10.relationship <> 'not-related-to')
        """).show();
```

</TabItem>
</Tabs>

## Supplying a resource

A ValueSet or ConceptMap that the terminology layer does not hold can be passed
as FHIR R4 JSON instead:

| Python                  | R                                | Java and Scala       |
| ----------------------- | -------------------------------- | -------------------- |
| `value_set_from_json`   | `pathling_value_set_from_json`   | `valueSetFromJson`   |
| `concept_map_from_json` | `pathling_concept_map_from_json` | `conceptMapFromJson` |

The resource must carry a `url`. Elements that R4 does not define are rejected
rather than ignored, so an R5 ConceptMap, such as the worked example of the
specification with its `relationship` and `noMap` elements, must be expressed
with the R4 `equivalence` (`unmatched` in place of `noMap`) before it is
supplied.

Where a supplied ValueSet carries an `expansion`, that expansion is the
membership and the terminology layer is not consulted. An incomplete expansion,
one that carries an `offset` or whose `total` exceeds the entries it lists, is
rejected. A ValueSet that carries only a `compose` is expanded by the
terminology layer. A supplied ConceptMap is converted directly.

```python
with open("cardiovascular-disease.ValueSet.json") as f:
    pc.value_set_from_json(f.read()).createOrReplaceTempView("cvd_codes")
```

## Limits and failures

The content of each DataFrame is held on the Spark driver, so its size is
capped: 100,000 members for a value set and 100,000 mappings for a concept map.
Each function takes an optional cap to raise or lower it (`max_members` and
`max_mappings` in Python and R, a second argument in Java and Scala).

A value set or concept map that cannot be resolved is an error, never an empty
DataFrame, so that a query never silently returns no rows because the
terminology was missing. The same applies where the membership cannot be
determined, where the version to use cannot be determined, where the concept
map carries content that the table cannot represent, and where the content
exceeds the cap. In Java and Scala the error is a `TerminologyRelationException`;
in Python and R it is raised from that exception and carries its message, which
names the value set or concept map and the reason.

The canonical URL and version resolved, the source, the member or mapping
count, and for a value set the expansion identifier, timestamp and code system
versions, are logged at `INFO` level by
`au.csiro.pathling.library.terminology.TerminologyRelations`.
