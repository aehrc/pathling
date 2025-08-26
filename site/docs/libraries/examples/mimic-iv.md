---
sidebar_position: 4
title: Querying MIMIC-IV data
description: Example of running queries over the MIMIC-IV on FHIR dataset using the Pathling libraries.
---

# Querying MIMIC-IV data

This article demonstrates how to extract and prepare clinical data from MIMIC-IV
using Pathling. We use
a [clinical study](https://jamanetwork.com/journals/jamainternalmedicine/fullarticle/2794196)
on oxygen supplementation differences between racial groups as our example,
focusing on the data preparation steps that transform raw healthcare records
into analysis-ready datasets.

This work was originally published as part of the
paper [SQL on FHIR - Tabular views of FHIR data using FHIRPath](https://www.nature.com/articles/s41746-025-01708-w)
published in [npj Digital Medicine](https://www.nature.com/npjdigitalmed/). The
full code is available in
the [aehrc/sql-on-fhir-evaluation](https://github.com/aehrc/sql-on-fhir-evaluation)
repository.

## Introduction

We demonstrate these data extraction techniques using a study that examined
whether patients from different racial and ethnic backgrounds receive different
amounts of supplemental oxygen in intensive care units. This study provides an
excellent example because it requires combining several types of clinical data:
patient demographics, vital signs measurements, oxygen delivery records, and
blood gas results.

Our data preparation process will extract:

1. Patient demographic information including race and ethnicity
2. Vital signs measurements, particularly oxygen saturation
3. Oxygen flow rate measurements from respiratory equipment
4. Blood gas analysis results showing oxygen levels in blood samples

## Importing the MIMIC-IV dataset

The MIMIC-IV dataset is provided in FHIR NDJSON format, and we can use the
NDJSON reader in Pathling to load it into a set of Spark dataframes. Because
MIMIC-IV uses a non-standard naming convention for its files, we need to provide
a custom file name mapper to correctly identify the resource type for each file:

```python
data = pc.read.ndjson(
    "/usr/share/staging/ndjson",
    file_name_mapper=lambda file_name: re.findall(r"Mimic(\w+?)(?:ED|ICU|"
                                                  r"Chartevents|Datetimeevents|Labevents|MicroOrg|MicroSusc|MicroTest|"
                                                  r"Outputevents|Lab|Mix|VitalSigns|VitalSignsED)?$",
                                                  file_name))
```

## Understanding the data extraction approach

### Layered data transformation

import MimicIv from '@site/src/images/mimic-iv.png';
import MimicIv2x from '@site/src/images/mimic-iv@2x.png';
import MimicIvDark from '@site/src/images/mimic-iv-dark.png';
import MimicIvDark2x from '@site/src/images/mimic-iv-dark@2x.png';

<img srcset={`${MimicIv2x} 2x, ${MimicIv} 1x`} title="Views used to transform
raw FHIR data into tables ready for analysis" className="light-mode-only"
width="800" />
<img srcset={`${MimicIvDark2x} 2x, ${MimicIvDark} 1x`} title="Views used to
transform raw FHIR data into tables ready for analysis" className="
dark-mode-only" width="800" />

Data is first extracted into a set of intermediate views using SQL on FHIR view
definitions. These views extract all relevant elements from the FHIR data.

Next, related measurements are combined into clinical concepts such as vital
signs and oxygen delivery using SQL transformations.

## SQL on FHIR views

The first step is to define SQL on FHIR views that extract relevant data from
the FHIR resources. Each view corresponds to a FHIR resource type and includes
only the fields needed for the analysis.

The view definitions are also capable of coercing the types of fields using
column tags. This ensures that timestamps, numeric values, and coded fields
are represented correctly for downstream analysis.

### Patient demographics (`rv_patient.json`)

This view extracts basic patient information including demographics and
race/ethnicity:

```json
{
    "name": "rv_patient",
    "resource": "Patient",
    "select": [
        {
            "column": [
                {
                    "name": "subject_id",
                    "path": "getResourceKey()",
                    "type": "string"
                },
                {
                    "name": "gender",
                    "path": "gender",
                    "type": "code"
                },
                {
                    "name": "race_code",
                    "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').extension('ombCategory').value.ofType(Coding).code",
                    "type": "code"
                },
                {
                    "name": "ethnicity_code",
                    "path": "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity').extension('ombCategory').value.ofType(Coding).code",
                    "type": "code"
                }
            ]
        }
    ]
}
```

**What this extracts**: Each patient's unique identifier, gender, and
racial/ethnic background. Race and ethnicity information is stored in
standardised extensions following US healthcare conventions.

**Why it matters**: Demographics are essential for health disparities research,
allowing researchers to examine whether different patient groups receive
different care or have different outcomes.

### ICU encounter details (`rv_icu_encounter.json`)

This view captures information about patients' stays in the intensive care unit:

```json
{
    "name": "rv_icu_encounter",
    "resource": "Encounter",
    "select": [
        {
            "column": [
                {
                    "name": "stay_id",
                    "path": "getResourceKey()",
                    "type": "string"
                },
                {
                    "name": "subject_id",
                    "path": "subject.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "admittime",
                    "path": "period.start",
                    "type": "dateTime",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "dischtime",
                    "path": "period.end",
                    "type": "dateTime",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                }
            ]
        }
    ],
    "where": [
        {
            "path": "class.code = 'ACUTE'"
        }
    ]
}
```

**What this extracts**: Each ICU stay with start and end times, filtered to
acute care encounters only.

**Why it matters**: This provides the time boundaries for each patient's ICU
stay, which helps researchers identify when medical interventions occurred and
calculate lengths of stay.

### Vital signs measurements (`rv_obs_vitalsigns.json`)

This view extracts recorded vital signs including heart rate, respiratory rate,
and oxygen saturation:

```json
{
    "name": "rv_obs_vitalsigns",
    "resource": "Observation",
    "select": [
        {
            "column": [
                {
                    "name": "subject_id",
                    "path": "subject.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "stay_id",
                    "path": "encounter.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "charttime",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "storetime",
                    "path": "issued",
                    "type": "instant",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "valuenum",
                    "path": "value.ofType(Quantity).value",
                    "type": "decimal"
                },
                {
                    "name": "itemid",
                    "path": "code.coding.code",
                    "type": "code",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "INTEGER"
                        }
                    ]
                }
            ]
        }
    ],
    "where": [
        {
            "path": "code.coding.code = '220045' or code.coding.code = '220277' or code.coding.code = '220210' or code.coding.code = '224690'"
        }
    ]
}
```

**What this extracts**: Measurements of heart rate (220045), oxygen saturation (
220277), and respiratory rate (220210, 224690), with timestamps and values.

**Why it matters**: Pulse oximetry readings (oxygen saturation) are crucial for
the study, as they show how well oxygen monitoring devices work for different
patient groups.

### Oxygen flow measurements (`rv_obs_o2_flow.json`)

This view captures oxygen flow rates from respiratory equipment:

```json
{
    "name": "rv_obs_o2_flow",
    "resource": "Observation",
    "select": [
        {
            "column": [
                {
                    "name": "subject_id",
                    "path": "subject.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "stay_id",
                    "path": "encounter.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "charttime",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "storetime",
                    "path": "issued",
                    "type": "instant",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "valuenum",
                    "path": "value.ofType(Quantity).value",
                    "type": "decimal"
                },
                {
                    "name": "itemid",
                    "path": "code.coding.code",
                    "type": "code",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "INTEGER"
                        }
                    ]
                }
            ]
        }
    ],
    "where": [
        {
            "path": "code.coding.code = '223834' or code.coding.code = '227582' or code.coding.code = '227287'"
        }
    ]
}
```

**What this extracts**: Oxygen flow rates in litres per minute from different
types of respiratory support equipment (regular oxygen flow, BiPAP oxygen flow,
and additional oxygen flow).

**Why it matters**: These measurements show how much supplemental oxygen each
patient received, which is the primary outcome being studied for racial
disparities.

### Oxygen delivery devices (`rv_o2_delivery_device.json`)

This view records what types of oxygen delivery equipment were used:

```json
{
    "name": "rv_o2_delivery_device",
    "resource": "Observation",
    "select": [
        {
            "column": [
                {
                    "name": "subject_id",
                    "path": "subject.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "stay_id",
                    "path": "encounter.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "charttime",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "value",
                    "path": "value.ofType(string)",
                    "type": "string"
                }
            ]
        }
    ],
    "where": [
        {
            "path": "code.coding.code = '226732'"
        }
    ]
}
```

**What this extracts**: Text descriptions of oxygen delivery devices (e.g., "
nasal cannula", "face mask", "mechanical ventilator").

**Why it matters**: Different delivery devices provide different amounts of
oxygen support, which helps researchers understand the intensity of treatment
each patient received.

### Blood gas measurements (`rv_obs_bg.json`)

This view extracts laboratory results from blood gas analyses:

```json
{
    "name": "rv_obs_bg",
    "resource": "Observation",
    "select": [
        {
            "column": [
                {
                    "name": "subject_id",
                    "path": "subject.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "hadm_id",
                    "path": "encounter.getReferenceKey()",
                    "type": "string"
                },
                {
                    "name": "charttime",
                    "path": "effective.ofType(dateTime)",
                    "type": "dateTime",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "storetime",
                    "path": "issued",
                    "type": "instant",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "TIMESTAMP"
                        }
                    ]
                },
                {
                    "name": "value",
                    "path": "value.ofType(string)",
                    "type": "string"
                },
                {
                    "name": "valuenum",
                    "path": "value.ofType(Quantity).value",
                    "type": "decimal"
                },
                {
                    "name": "itemid",
                    "path": "code.coding.code",
                    "type": "code",
                    "tag": [
                        {
                            "name": "ansi/type",
                            "value": "INTEGER"
                        }
                    ]
                },
                {
                    "name": "specimen_id",
                    "path": "specimen.getReferenceKey()",
                    "type": "string"
                }
            ]
        }
    ],
    "where": [
        {
            "path": "code.coding.code = '52033' or code.coding.code = '50817' or code.coding.code = '50818'"
        }
    ]
}
```

**What this extracts**: Laboratory results showing oxygen saturation measured
directly from blood samples (50817), carbon dioxide levels (50818), and specimen
information (52033).

**Why it matters**: Blood gas measurements provide the "gold standard" for
measuring oxygen levels, which researchers compare against pulse oximeter
readings to assess device accuracy.

## Building clinical concepts from SQL on FHIR views

The next step combines related measurements from the SQL on FHIR views into
familiar
clinical concepts. This process transforms individual observations into the
types
of measurements that clinicians and researchers typically work with.

### Vital signs processing (`md_vitalsigns.sql`)

This step takes the individual vital sign measurements and combines them into a
single table with clean, validated values:

```sql
CREATE TABLE md_vitalsigns AS
SELECT ce.subject_id,
       ce.stay_id,
       ce.charttime,
       AVG(CASE
               WHEN itemid IN (220045)
                   AND valuenum > 0 AND valuenum < 300
                   THEN valuenum END) AS heart_rate,
       AVG(CASE
               WHEN itemid IN (220210, 224690)
                   AND valuenum > 0 AND valuenum < 70
                   THEN valuenum END) AS resp_rate,
       AVG(CASE
               WHEN itemid IN (220277)
                   AND valuenum > 0 AND valuenum <= 100
                   THEN valuenum END) AS spo2
FROM rv_obs_vitalsigns ce
WHERE ce.stay_id IS NOT NULL
GROUP BY ce.subject_id, ce.stay_id, ce.charttime;
```

**What this accomplishes**:

- Groups measurements by patient, stay, and time
- Applies clinical range validation (e.g., heart rate between 0-300, oxygen
  saturation 0-100%)
- Averages multiple measurements taken at the same time
- Creates clean columns for each vital sign type

**Why this matters**: Raw medical device data often contains invalid readings
due to equipment malfunctions or patient movement. This step filters out clearly
erroneous values and provides clinically meaningful measurements.

### Oxygen delivery processing (`md_oxygen_delivery.sql`)

This creates a comprehensive view of oxygen therapy by combining flow rates and
delivery device information:

```sql
CREATE TABLE md_oxygen_delivery AS
WITH ce_stg1 AS (SELECT ce.subject_id,
                        ce.stay_id,
                        ce.charttime,
                        -- Combine similar oxygen flow measurements
                        CASE
                            WHEN itemid IN (223834, 227582) THEN 223834
                            ELSE itemid END AS itemid,
                        valuenum
                 FROM rv_obs_o2_flow ce
                 WHERE ce.valuenum IS NOT NULL),
-- Additional processing steps...
     SELECT
    subject_id,
    MAX
(
    stay_id
) AS stay_id,
    charttime,
    MAX
(
    CASE
    WHEN
    itemid =
    223834
    THEN
    valuenum
    END
) AS o2_flow,
    MAX
(
    CASE
    WHEN
    itemid =
    227287
    THEN
    valuenum
    END
) AS o2_flow_additional,
    MAX
(
    CASE
    WHEN
    rn =
    1
    THEN
    o2_device
    END
) AS o2_delivery_device_1
    -- Up to 4 devices can be used simultaneously
    FROM combined_data
    GROUP BY subject_id, charttime;
```

**What this accomplishes**:

- Combines different types of oxygen flow measurements
- Handles cases where patients use multiple oxygen devices simultaneously
- Prioritises the most recent measurement when multiple values exist for the
  same time
- Creates separate columns for main and additional oxygen flows

**Why this matters**: Patients may receive oxygen through multiple devices at
once (e.g., nasal cannula plus mechanical ventilator). This processing ensures
researchers capture the complete picture of oxygen therapy.

### Blood gas analysis processing (`md_bg.sql`)

This step processes laboratory blood gas results, which provide the most
accurate oxygen measurements:

```sql
CREATE TABLE md_bg AS
SELECT MAX(subject_id)                                 AS subject_id,
       MAX(hadm_id)                                    AS hadm_id,
       MAX(charttime)                                  AS charttime,
       MAX(CASE WHEN itemid = 52033 THEN value END)    AS specimen,
       MAX(CASE
               WHEN itemid = 50817 AND valuenum <= 100
                   THEN valuenum END)                  AS so2,
       MAX(CASE WHEN itemid = 50818 THEN valuenum END) AS pco2
FROM rv_obs_bg le
WHERE le.itemid IN (52033, 50817, 50818, -- additional blood gas parameters
    GROUP BY le . specimen_id;
```

**What this accomplishes**:

- Groups all measurements from the same blood sample together
- Extracts oxygen saturation (so2) with clinical validation (≤100%)
- Separates different types of blood gas measurements into distinct columns
- Ensures each blood sample contributes only one row to the final dataset

**Why this matters**: Blood gas analyses involve multiple measurements from a
single blood draw. This processing reconstructs the complete results for each
sample, providing the "gold standard" oxygen measurements needed for comparison
with pulse oximetry.

## Creating study-specific datasets

The final transformation step filters and combines the clinical concepts to
create datasets that directly answer the research questions.

### Defining the study population (`st_subject.sql`)

This step identifies which patients and time periods to include in the analysis:

```sql
CREATE
OR REPLACE TEMP VIEW st_subject AS
WITH vent_intervention AS (
    SELECT stay_id,
        charttime AS inttime,
        ventilation_status AS int_type,
        row_number() OVER (PARTITION BY stay_id ORDER BY charttime) AS int_sequence
    FROM st_ventilation
    WHERE ventilation_status NOT in ('None', 'SupplementalOxygen')
        AND ventilation_status IS NOT NULL
),
first_vent_intervention AS (
    SELECT * FROM vent_intervention WHERE int_sequence = 1
),
stay_with_index_period AS (
    SELECT subject_id,
        stay_id,
        gender,
        race AS race_category,
        admittime AS ip_starttime,
        GREATEST(admittime, LEAST(dischtime, inttime, admittime + interval '5 days')) AS ip_endtime
    FROM first_icu_stay_with_intervention
)
SELECT subject_id, stay_id, gender, race_category, ip_starttime, ip_endtime
FROM stay_with_index_period
WHERE race_category IS NOT NULL
  AND (ip_endtime - ip_starttime) >= INTERVAL '12 hours';
```

**What this accomplishes**:

- Identifies the first mechanical ventilation event for each patient
- Creates a 5-day study window starting from ICU admission
- Excludes patients with missing race/ethnicity information
- Requires at least 12 hours of data for meaningful analysis

**Why this matters**: Clear inclusion criteria ensure the study examines
comparable patients and time periods, reducing bias and improving the validity
of comparisons between racial groups.

### Extracting oxygen flow measurements (`st_reading_o2_flow.sql`)

This creates the primary outcome dataset showing how much oxygen each patient
received:

```sql
CREATE
OR REPLACE TEMP VIEW st_reading_o2_flow AS
SELECT sbj.subject_id,
       odd.charttime as chart_time,
       odd.o2_flow
FROM st_subject AS sbj
         JOIN md_oxygen_delivery AS odd ON sbj.stay_id = odd.stay_id
WHERE odd.charttime BETWEEN sbj.ip_starttime AND sbj.ip_endtime
  AND odd.o2_flow IS NOT NULL
```

**What this accomplishes**:

- Links oxygen measurements to the study population
- Filters measurements to the defined study time windows
- Excludes missing or invalid oxygen flow values

### Extracting pulse oximetry readings (`st_reading_spo2.sql`)

This creates the dataset of oxygen saturation measured by pulse oximeters:

```sql
CREATE
OR REPLACE TEMP VIEW st_reading_spo2 AS
SELECT sbj.subject_id,
       vs.charttime as chart_time,
       vs.spo2
FROM st_subject AS sbj
         JOIN md_vitalsigns AS vs ON sbj.stay_id = vs.stay_id
WHERE vs.charttime BETWEEN sbj.ip_starttime AND sbj.ip_endtime
  AND vs.spo2 IS NOT NULL
```

### Extracting blood oxygen measurements (`st_reading_so2.sql`)

This creates the dataset of oxygen saturation measured from blood samples:

```sql
CREATE
OR REPLACE TEMP VIEW st_reading_so2 AS
SELECT sbj.subject_id,
       bg.charttime as chart_time,
       bg.so2
FROM st_subject AS sbj
         JOIN md_bg AS bg ON sbj.subject_id = bg.subject_id
WHERE bg.charttime BETWEEN sbj.ip_starttime AND sbj.ip_endtime
  AND bg.so2 IS NOT NULL
```

**What these accomplish**: Each query creates a time series of measurements for
the study population, filtered to the relevant time periods and excluding
missing values.

**Why this matters**: These datasets enable direct comparison between pulse
oximeter readings (SpO₂) and blood gas measurements (SaO₂), which is essential
for assessing whether pulse oximeters work equally well for all patient groups.

## Running the data extraction process

The data extraction system processes these layered views using Pathling's
datasources API.

### Processing the view definitions

With the datasource already loaded, we can execute the SQL on FHIR view
definitions and register them as Spark temporary views for further processing:

```python
import json
import os

# Process SQL on FHIR views
view_definitions = [
    "rv_patient", "rv_icu_encounter", "rv_obs_vitalsigns",
    "rv_obs_o2_flow", "rv_o2_delivery_device", "rv_obs_bg"
]

for view_name in view_definitions:
    view_path = f"views/sof/{view_name}.json"
    with open(view_path) as f:
        view_json = f.read()
        # Execute the view definition
        df = datasource.view(json=view_json)
        # Register as temporary view for SQL processing
        df.createOrReplaceTempView(view_name)

# Process clinical concept views using SQL
clinical_views = ["md_vitalsigns", "md_oxygen_delivery", "md_bg"]

for view_name in clinical_views:
    sql_path = f"views/clinical/{view_name}.sql"
    with open(sql_path) as f:
        sql_query = f.read()
        # Execute SQL and register result
        spark.sql(sql_query).createOrReplaceTempView(view_name)
```

**What this accomplishes**:

- The `datasource.view(json=...)` method executes each SQL on FHIR view
  definition, handling type hints automatically
- Results are registered as Spark temporary views, making them available for SQL
  queries
- Clinical concept views can then reference these base views in their SQL

### Running study-specific views and exporting to CSV

The final step executes the study-specific SQL views and exports the results to
CSV files using Spark's CSV writer:

```python
# Execute study-specific views and export to CSV
study_views = ["st_subject", "st_reading_o2_flow", "st_reading_spo2",
               "st_reading_so2"]
output_directory = "output"

for view_name in study_views:
    sql_path = f"views/study/{view_name}.sql"
    with open(sql_path) as f:
        sql_query = f.read()
        # Execute the SQL query
        df = spark.sql(sql_query)

        # Export to CSV with proper naming
        csv_name = view_name.replace("st_", "")  # Remove "st_" prefix
        output_path = f"{output_directory}/{csv_name}"

        df.write.mode("overwrite")
        .option("header", "true")
        .csv(output_path)

print("Data extraction complete. CSV files saved to output directory.")
```

**What this accomplishes**:

- Executes each study-specific SQL view in sequence
- Uses Spark's native CSV writer with headers enabled
- Overwrites existing files to ensure clean output
- Creates properly named CSV files ready for analysis

### Output datasets

The extraction process creates five CSV files ready for analysis:

1. **`subject.csv`**: Study population with demographics
    - `subject_id`: Patient identifier
    - `gender`: Patient gender
    - `race_category`: Racial/ethnic group
    - `ip_starttime`, `ip_endtime`: Study observation period

2. **`reading_o2_flow.csv`**: Oxygen delivery measurements
    - `subject_id`: Patient identifier
    - `chart_time`: When measurement was recorded
    - `o2_flow`: Oxygen flow rate in litres per minute

3. **`reading_spo2.csv`**: Pulse oximetry readings
    - `subject_id`: Patient identifier
    - `chart_time`: When measurement was taken
    - `spo2`: Oxygen saturation percentage from pulse oximeter

4. **`reading_so2.csv`**: Blood gas oxygen measurements
    - `subject_id`: Patient identifier
    - `chart_time`: When blood sample was drawn
    - `so2`: Oxygen saturation percentage from blood analysis

5. **`ventilation.csv`**: Mechanical ventilation status
    - `subject_id`: Patient identifier
    - `chart_time`: When status was recorded
    - `ventilation_status`: Type of respiratory support

## Further reading

- [SQL on FHIR - Tabular views of FHIR data using FHIRPath](https://www.nature.com/articles/s41746-025-01708-w) -
  Original research paper demonstrating these techniques
- [aehrc/sql-on-fhir-evaluation](https://github.com/aehrc/sql-on-fhir-evaluation) -
  Full code repository for this example
- [SQL on FHIR example](/docs/libraries/examples/prostate-cancer.md) - More
  examples of SQL on FHIR queries
