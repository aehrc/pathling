# Classifying medicines with a concept map

The SQL on FHIR [Terminology in SQL](https://build.fhir.org/ig/HL7/sql-on-fhir/en/terminology-in-sql.html) page defines how the content of a ValueSet or ConceptMap is presented to a SQL query: as a table that the query joins to, like any other. This example uses that approach to classify the medicines in a set of FHIR records, then answers two questions with SQL:

* How many patients are on an active medicine in each class?
* Which patients are on a bone resorption inhibitor but not on calcium?

The terminology is answered from a [local terminology store](/docs/libraries/terminology/local.md), so no terminology server is needed. The columns of the relation and the functions that create it are described in [Terminology in SQL](/docs/libraries/terminology/sql.md).

## The data[​](#the-data "Direct link to The data")

The example uses two inputs:

* **FHIR resources** in NDJSON format: Patient, MedicationStatement and MedicationRequest resources, with medicines coded using the Australian Medicines Terminology (AMT), which is part of SNOMED CT-AU.
* **A ConceptMap** that relates AMT products to the classes they belong to, such as `373301002 |Bone resorption inhibitor|` and `767178007 |Calcium and/or calcium compound|`. A product may belong to many classes, and each class is a separate mapping with the equivalence `relatedto`.

The results below come from a synthetic cohort of 1,000 patients and a map of 925,540 mappings, with the canonical `https://example.org/amt-extended-91b6e2c4/ConceptMap/product-to-classification|0.1.0`.

## Importing the concept map[​](#importing-the-concept-map "Direct link to Importing the concept map")

Import the ConceptMap into a local terminology store. This is done once; the store can then be reused by any number of sessions.

* Python
* R
* CLI

```
from pathling import PathlingContext

PathlingContext.create().import_fhir_terminology(
    "product-to-classification.json", "tx-store"
)
```

```
library(pathling)

pc <- pathling_connect()
pathling_import_fhir_terminology(pc, "product-to-classification.json", "tx-store")
pc %>% pathling_disconnect()
```

```
pathling import-fhir-terminology product-to-classification.json tx-store
```

## Flattening the resources[​](#flattening-the-resources "Direct link to Flattening the resources")

Create a context in local terminology mode, pointing at the store. The concept map is read into the driver when its relation is created and held in the relation, so a map of this size needs a larger driver heap than the default (1 GB in Python, 2 GB in R); 4 GB was sufficient here.

Each resource type is then flattened with a [ViewDefinition](https://build.fhir.org/ig/HL7/sql-on-fhir/en/StructureDefinition-ViewDefinition.html), and the result is registered as a temporary view so that SQL can refer to it by name. The ViewDefinition for MedicationStatement takes the SNOMED CT code of the medicine:

```
{
    "resourceType": "ViewDefinition",
    "name": "medication_statement",
    "resource": "MedicationStatement",
    "select": [
        {
            "column": [
                { "name": "id", "path": "getResourceKey()" },
                {
                    "name": "patient_id",
                    "path": "subject.getReferenceKey(Patient)"
                },
                { "name": "status", "path": "status" },
                {
                    "name": "code",
                    "path": "medication.ofType(CodeableConcept).coding.where(system = 'http://snomed.info/sct').code.first()"
                },
                {
                    "name": "display",
                    "path": "medication.ofType(CodeableConcept).coding.where(system = 'http://snomed.info/sct').display.first()"
                }
            ]
        }
    ]
}
```

The `medication_request` view has the same columns for MedicationRequest, and the `patient` view includes `id`, `given` and `family`.

* Python
* R

```
from pathling import PathlingContext

pc = PathlingContext.create(
    spark_conf={"spark.driver.memory": "4g"},
    terminology_mode="local",
    terminology_storage_path="tx-store",
)

data = pc.read.ndjson("fhir")
for name in ["patient", "medication_statement", "medication_request"]:
    with open(f"views/{name}.json") as f:
        data.view(json=f.read()).createOrReplaceTempView(name)
```

```
library(sparklyr)
library(pathling)

sc <- spark_connect(
  master = "local[*]",
  config = list(
    "sparklyr.shell.driver-memory" = "4g",
    "sparklyr.shell.conf" = c(
      "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
  ),
  version = pathling_spark_info()$spark_version
)
pc <- pathling_connect(
  spark = sc,
  terminology_mode = "local",
  terminology_storage_path = "tx-store"
)
spark <- pathling_spark(pc)

data <- pathling_read_ndjson(pc, "fhir")
views <- c(
  patient = "Patient",
  medication_statement = "MedicationStatement",
  medication_request = "MedicationRequest"
)
for (name in names(views)) {
  json <- paste(readLines(file.path("views", paste0(name, ".json"))), collapse = "\n")
  sdf_register(ds_view(data, views[[name]], json = json), name)
}
```

## Registering the concept map[​](#registering-the-concept-map "Direct link to Registering the concept map")

The concept map relation is registered under the name that the SQL uses for it, `product_to_class`. In a SQL on FHIR SQLView, this name would be the label of the ConceptMap's `depends-on` dependency; here, registering it is all that is needed.

The map has more mappings than the default cap of 100,000, so the cap is raised. A map that exceeds the cap is rejected rather than truncated.

* Python
* R

```
pc.concept_map(
    "https://example.org/amt-extended-91b6e2c4/ConceptMap/product-to-classification|0.1.0",
    max_mappings=1_000_000,
).createOrReplaceTempView("product_to_class")
```

```
product_to_class <- pathling_concept_map(
  pc,
  "https://example.org/amt-extended-91b6e2c4/ConceptMap/product-to-classification|0.1.0",
  max_mappings = 1000000
)
sdf_register(product_to_class, "product_to_class")
```

The relation has one row per mapping. Its first rows show one product with several classes (version columns omitted):

| source\_code     | source\_display                            | target\_code | target\_display               | relationship |
| ---------------- | ------------------------------------------ | ------------ | ----------------------------- | ------------ |
| 1000001000168107 | Rivaroxaban (Apo) 10 mg tablet, 30, bottle | 106205006    | Haemostasis related substance | related-to   |
| 1000001000168107 | Rivaroxaban (Apo) 10 mg tablet, 30, bottle | 1091008      | Coagulation factor inhibitor  | related-to   |
| 1000001000168107 | Rivaroxaban (Apo) 10 mg tablet, 30, bottle | 12878005     | Thiophene                     | related-to   |

The `relationship` column holds the R5 code `related-to`, converted from the R4 equivalence `relatedto` in the ConceptMap.

## Classifying the medicines[​](#classifying-the-medicines "Direct link to Classifying the medicines")

A view combines the statements and requests, keeps the active ones, and joins each medicine to its classes. A medicine in several classes produces several rows, and a medicine that the map does not classify produces none.

* Python
* R

```
pc.spark.sql("""
    CREATE OR REPLACE TEMP VIEW medication_class AS
    WITH medication AS (
      SELECT id AS medication_id, patient_id, status, code, display
      FROM medication_statement
      UNION ALL
      SELECT id, patient_id, status, code, display
      FROM medication_request
    )
    SELECT m.medication_id,
           m.patient_id,
           m.code,
           m.display,
           c.target_code AS class_code,
           c.target_display AS class_display
    FROM medication m
    JOIN product_to_class c
      ON c.source_system = 'http://snomed.info/sct'
     AND c.source_code = m.code
     AND c.relationship = 'related-to'
    WHERE m.status = 'active'
""")
```

```
invisible(sdf_sql(spark, "
    CREATE OR REPLACE TEMP VIEW medication_class AS
    WITH medication AS (
      SELECT id AS medication_id, patient_id, status, code, display
      FROM medication_statement
      UNION ALL
      SELECT id, patient_id, status, code, display
      FROM medication_request
    )
    SELECT m.medication_id,
           m.patient_id,
           m.code,
           m.display,
           c.target_code AS class_code,
           c.target_display AS class_display
    FROM medication m
    JOIN product_to_class c
      ON c.source_system = 'http://snomed.info/sct'
     AND c.source_code = m.code
     AND c.relationship = 'related-to'
    WHERE m.status = 'active'
"))
```

## Patients in each class[​](#patients-in-each-class "Direct link to Patients in each class")

A patient is counted once in every class reached by any of their active medicines:

* Python
* R

```
pc.spark.sql("""
    SELECT class_code,
           class_display,
           COUNT(DISTINCT patient_id) AS patients,
           COUNT(DISTINCT code) AS products
    FROM medication_class
    GROUP BY class_code, class_display
    ORDER BY patients DESC, class_code
""").show(10, truncate=False)
```

```
sdf_sql(spark, "
    SELECT class_code,
           class_display,
           COUNT(DISTINCT patient_id) AS patients,
           COUNT(DISTINCT code) AS products
    FROM medication_class
    GROUP BY class_code, class_display
    ORDER BY patients DESC, class_code
") %>% head(10)
```

| class\_code | class\_display                          | patients | products |
| ----------- | --------------------------------------- | -------- | -------- |
| 372586001   | Antihypertensive agent                  | 429      | 288      |
| 866173006   | Renin-angiotensin system inhibitor      | 379      | 253      |
| 372912004   | HMG-CoA reductase inhibitor             | 318      | 181      |
| 373267003   | Antilipaemic agent                      | 318      | 181      |
| 372733002   | Angiotensin-converting enzyme inhibitor | 311      | 169      |
| 373258003   | Gastrointestinal agent                  | 277      | 123      |
| 372525000   | Proton pump inhibitor                   | 274      | 120      |
| 372590004   | Ulcer healing agent                     | 274      | 120      |
| 87918000    | Mineral                                 | 186      | 78       |
| 31212001    | Calcium compound                        | 184      | 76       |

## Bone resorption inhibitors without calcium[​](#bone-resorption-inhibitors-without-calcium "Direct link to Bone resorption inhibitors without calcium")

This query finds the patients on a medicine in the class `373301002` who are on no medicine in the class `767178007`. The test for calcium is a `NOT EXISTS` over the same view, so it uses the classes that the map assigns, not a list of products written into the query.

* Python
* R

```
pc.spark.sql("""
    SELECT p.id AS patient_id,
           p.given,
           p.family,
           string_agg(DISTINCT m.display, '; ') AS bone_resorption_inhibitors
    FROM medication_class m
    JOIN patient p ON p.id = m.patient_id
    WHERE m.class_code = '373301002'
      AND NOT EXISTS (
        SELECT 1 FROM medication_class c
        WHERE c.patient_id = m.patient_id AND c.class_code = '767178007'
      )
    GROUP BY p.id, p.given, p.family
    ORDER BY p.id
""").show(5, truncate=False)
```

```
sdf_sql(spark, "
    SELECT p.id AS patient_id,
           p.given,
           p.family,
           string_agg(DISTINCT m.display, '; ') AS bone_resorption_inhibitors
    FROM medication_class m
    JOIN patient p ON p.id = m.patient_id
    WHERE m.class_code = '373301002'
      AND NOT EXISTS (
        SELECT 1 FROM medication_class c
        WHERE c.patient_id = m.patient_id AND c.class_code = '767178007'
      )
    GROUP BY p.id, p.given, p.family
    ORDER BY p.id
") %>% head(5)

pc %>% pathling_disconnect()
```

The query returns 33 patients, the first five of which are:

| patient\_id         | given    | family   | bone\_resorption\_inhibitors                                        |
| ------------------- | -------- | -------- | ------------------------------------------------------------------- |
| Patient/cohort-0012 | Glenys   | SINGH    | Alendrobell Plus D3 70 mg/140 microgram tablet, 4                   |
| Patient/cohort-0038 | Beverley | ANDERSON | Raloxifene hydrochloride 60 mg tablet                               |
| Patient/cohort-0041 | Pauline  | WILSON   | Alendronate 10 mg tablet                                            |
| Patient/cohort-0075 | Pauline  | JOHNSON  | Alendronate 70 mg + colecalciferol 70 microgram (2800 units) tablet |
| Patient/cohort-0085 | Dianne   | KELLY    | Risedronate sodium 35 mg tablet                                     |

## Further reading[​](#further-reading "Direct link to Further reading")

* [Terminology in SQL](/docs/libraries/terminology/sql.md) - the value set and concept map relations, and supplying a resource as JSON
* [Local terminology mode](/docs/libraries/terminology/local.md) - importing content into a local terminology store
* [Running queries](/docs/libraries/running-queries.md) - executing ViewDefinitions
* [Terminology in SQL](https://build.fhir.org/ig/HL7/sql-on-fhir/en/terminology-in-sql.html) - the SQL on FHIR specification page
