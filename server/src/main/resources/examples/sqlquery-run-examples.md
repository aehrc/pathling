# $sqlquery-run Operation Examples

These examples demonstrate the `$sqlquery-run` operation alongside the
existing `$viewdefinition-run` operation. They use a small synthetic dataset
of Patients and Conditions.

## Prerequisites

Start the server with a writable warehouse directory:

```bash
WAREHOUSE_DIR=$(mktemp -d)/warehouse
mkdir -p "$WAREHOUSE_DIR"

PATHLING_STORAGE_WAREHOUSEURL="file://${WAREHOUSE_DIR}" \
PATHLING_STORAGE_DATABASENAME="default" \
mvn spring-boot:run
```

## 1. Load Test Data

### Patients

```bash
for p in \
  '{"resourceType":"Patient","id":"pat-1","gender":"male","birthDate":"1990-05-15","name":[{"use":"official","family":"Smith","given":["John"]}]}' \
  '{"resourceType":"Patient","id":"pat-2","gender":"female","birthDate":"1985-11-22","name":[{"use":"official","family":"Johnson","given":["Jane"]}]}' \
  '{"resourceType":"Patient","id":"pat-3","gender":"male","birthDate":"2000-03-08","name":[{"use":"official","family":"Williams","given":["Bob"]}]}' \
  '{"resourceType":"Patient","id":"pat-4","gender":"female","birthDate":"1978-07-30","name":[{"use":"official","family":"Brown","given":["Alice"]}]}' \
  '{"resourceType":"Patient","id":"pat-5","gender":"male","birthDate":"1995-12-01","name":[{"use":"official","family":"Davis","given":["Charlie"]}]}'
do
  id=$(echo "$p" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
  curl -s -o /dev/null -w "Patient/$id: %{http_code}\n" \
    -X PUT "http://localhost:8080/fhir/Patient/$id" \
    -H "Content-Type: application/fhir+json" -d "$p"
done
```

### Conditions

```bash
for c in \
  '{"resourceType":"Condition","id":"cnd-1","subject":{"reference":"Patient/pat-1"},"code":{"coding":[{"system":"http://snomed.info/sct","code":"73211009","display":"Diabetes mellitus"}]},"onsetDateTime":"2015-03-12"}' \
  '{"resourceType":"Condition","id":"cnd-2","subject":{"reference":"Patient/pat-1"},"code":{"coding":[{"system":"http://snomed.info/sct","code":"38341003","display":"Hypertension"}]},"onsetDateTime":"2018-06-01"}' \
  '{"resourceType":"Condition","id":"cnd-3","subject":{"reference":"Patient/pat-2"},"code":{"coding":[{"system":"http://snomed.info/sct","code":"195967001","display":"Asthma"}]},"onsetDateTime":"2010-09-20"}' \
  '{"resourceType":"Condition","id":"cnd-4","subject":{"reference":"Patient/pat-3"},"code":{"coding":[{"system":"http://snomed.info/sct","code":"73211009","display":"Diabetes mellitus"}]},"onsetDateTime":"2020-01-15"}' \
  '{"resourceType":"Condition","id":"cnd-5","subject":{"reference":"Patient/pat-4"},"code":{"coding":[{"system":"http://snomed.info/sct","code":"38341003","display":"Hypertension"}]},"onsetDateTime":"2012-11-30"}' \
  '{"resourceType":"Condition","id":"cnd-6","subject":{"reference":"Patient/pat-5"},"code":{"coding":[{"system":"http://snomed.info/sct","code":"195967001","display":"Asthma"}]},"onsetDateTime":"2019-04-10"}'
do
  id=$(echo "$c" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
  curl -s -o /dev/null -w "Condition/$id: %{http_code}\n" \
    -X PUT "http://localhost:8080/fhir/Condition/$id" \
    -H "Content-Type: application/fhir+json" -d "$c"
done
```

### ViewDefinitions

Create a Patient ViewDefinition:

```bash
curl -s -X POST "http://localhost:8080/fhir/ViewDefinition" \
  -H "Content-Type: application/fhir+json" \
  -d '{
    "resourceType": "ViewDefinition",
    "name": "patients",
    "resource": "Patient",
    "select": [
      {
        "column": [
          {"path": "id", "name": "patient_id"},
          {"path": "gender", "name": "gender"},
          {"path": "birthDate", "name": "birth_date"}
        ]
      },
      {
        "forEach": "name.where(use = '\''official'\'')",
        "column": [
          {"path": "family", "name": "family_name"},
          {"path": "given.first()", "name": "given_name"}
        ]
      }
    ]
  }'
```

Create a Condition ViewDefinition:

```bash
curl -s -X POST "http://localhost:8080/fhir/ViewDefinition" \
  -H "Content-Type: application/fhir+json" \
  -d '{
    "resourceType": "ViewDefinition",
    "name": "conditions",
    "resource": "Condition",
    "select": [
      {
        "column": [
          {"path": "id", "name": "condition_id"},
          {"path": "subject.reference", "name": "patient_ref"},
          {"path": "code.coding.first().code", "name": "snomed_code"},
          {"path": "code.coding.first().display", "name": "condition_name"},
          {"path": "onsetDateTime", "name": "onset_date"}
        ]
      }
    ]
  }'
```

Note the IDs returned in the responses. Replace `<PATIENT_VD_ID>` and
`<CONDITION_VD_ID>` in the examples below with these values. You can also
look them up:

```bash
curl -s "http://localhost:8080/fhir/ViewDefinition?_count=10" \
  -H "Accept: application/fhir+json" | python3 -m json.tool
```

## 2. $viewdefinition-run Examples

### Instance-level $run on a stored ViewDefinition

```bash
curl -s "http://localhost:8080/fhir/ViewDefinition/<PATIENT_VD_ID>/\$run" \
  -H "Accept: application/x-ndjson"
```

Expected output (NDJSON):

```
{"patient_id":"pat-1","gender":"male","birth_date":"1990-05-15","family_name":"Smith","given_name":"John"}
{"patient_id":"pat-2","gender":"female","birth_date":"1985-11-22","family_name":"Johnson","given_name":"Jane"}
{"patient_id":"pat-3","gender":"male","birth_date":"2000-03-08","family_name":"Williams","given_name":"Bob"}
{"patient_id":"pat-4","gender":"female","birth_date":"1978-07-30","family_name":"Brown","given_name":"Alice"}
{"patient_id":"pat-5","gender":"male","birth_date":"1995-12-01","family_name":"Davis","given_name":"Charlie"}
```

### Stored Condition ViewDefinition as CSV

```bash
curl -s "http://localhost:8080/fhir/ViewDefinition/<CONDITION_VD_ID>/\$run?_format=csv"
```

Expected output:

```
condition_id,patient_ref,snomed_code,condition_name,onset_date
cnd-1,Patient/pat-1,73211009,Diabetes mellitus,2015-03-12
cnd-2,Patient/pat-1,38341003,Hypertension,2018-06-01
cnd-3,Patient/pat-2,195967001,Asthma,2010-09-20
cnd-4,Patient/pat-3,73211009,Diabetes mellitus,2020-01-15
cnd-5,Patient/pat-4,38341003,Hypertension,2012-11-30
cnd-6,Patient/pat-5,195967001,Asthma,2019-04-10
```

### Inline ViewDefinition with inline resources (no stored data needed)

```bash
curl -s -X POST "http://localhost:8080/fhir/\$viewdefinition-run" \
  -H "Content-Type: application/fhir+json" \
  -H "Accept: application/x-ndjson" \
  -d '{
    "resourceType": "Parameters",
    "parameter": [
      {
        "name": "viewResource",
        "resource": {
          "resourceType": "ViewDefinition",
          "name": "patient_demographics",
          "resource": "Patient",
          "select": [
            {
              "column": [
                {"path": "id", "name": "patient_id"},
                {"path": "gender", "name": "gender"},
                {"path": "birthDate", "name": "birth_date"}
              ]
            },
            {
              "forEach": "name.where(use = '\''official'\'')",
              "column": [
                {"path": "family", "name": "family_name"},
                {"path": "given.first()", "name": "given_name"}
              ]
            }
          ]
        }
      },
      {
        "name": "resource",
        "valueString": "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"gender\":\"male\",\"birthDate\":\"1990-05-15\",\"name\":[{\"use\":\"official\",\"family\":\"Smith\",\"given\":[\"John\"]}]}"
      },
      {
        "name": "resource",
        "valueString": "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"gender\":\"female\",\"birthDate\":\"1985-11-22\",\"name\":[{\"use\":\"official\",\"family\":\"Johnson\",\"given\":[\"Jane\"]}]}"
      }
    ]
  }'
```

## 3. $sqlquery-run Examples

The `$sqlquery-run` operation takes a Library resource conforming to the
SQLQuery profile. The Library contains Base64-encoded SQL in its `content`,
and references stored ViewDefinitions via `relatedArtifact`. The `label` on
each artifact becomes the table name used in the SQL.

### JOIN patients with conditions

SQL:

```sql
SELECT p.given_name, p.family_name, p.gender, p.birth_date,
       c.condition_name, c.onset_date
FROM patients p
JOIN conditions c ON concat('Patient/', p.patient_id) = c.patient_ref
ORDER BY p.family_name, c.onset_date
```

```bash
SQL="SELECT p.given_name, p.family_name, p.gender, p.birth_date, c.condition_name, c.onset_date FROM patients p JOIN conditions c ON concat('Patient/', p.patient_id) = c.patient_ref ORDER BY p.family_name, c.onset_date"
SQL_B64=$(echo -n "$SQL" | base64)

curl -s -X POST "http://localhost:8080/fhir/\$sqlquery-run?_format=csv" \
  -H "Content-Type: application/fhir+json" \
  -d "{
    \"resourceType\": \"Parameters\",
    \"parameter\": [{
      \"name\": \"queryResource\",
      \"resource\": {
        \"resourceType\": \"Library\",
        \"status\": \"active\",
        \"type\": {\"coding\": [{\"system\": \"https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes\", \"code\": \"sql-query\"}]},
        \"content\": [{
          \"contentType\": \"application/sql\",
          \"data\": \"${SQL_B64}\"
        }],
        \"relatedArtifact\": [
          {\"type\": \"depends-on\", \"label\": \"patients\", \"resource\": \"ViewDefinition/<PATIENT_VD_ID>\"},
          {\"type\": \"depends-on\", \"label\": \"conditions\", \"resource\": \"ViewDefinition/<CONDITION_VD_ID>\"}
        ]
      }
    }]
  }"
```

Expected output:

```
given_name,family_name,gender,birth_date,condition_name,onset_date
Alice,Brown,female,1978-07-30,Hypertension,2012-11-30
Charlie,Davis,male,1995-12-01,Asthma,2019-04-10
Jane,Johnson,female,1985-11-22,Asthma,2010-09-20
John,Smith,male,1990-05-15,Diabetes mellitus,2015-03-12
John,Smith,male,1990-05-15,Hypertension,2018-06-01
Bob,Williams,male,2000-03-08,Diabetes mellitus,2020-01-15
```

### Aggregate: patients per condition

SQL:

```sql
SELECT c.condition_name, count(*) AS patient_count
FROM conditions c
GROUP BY c.condition_name
ORDER BY patient_count DESC
```

```bash
SQL="SELECT c.condition_name, count(*) AS patient_count FROM conditions c GROUP BY c.condition_name ORDER BY patient_count DESC"
SQL_B64=$(echo -n "$SQL" | base64)

curl -s -X POST "http://localhost:8080/fhir/\$sqlquery-run?_format=csv" \
  -H "Content-Type: application/fhir+json" \
  -d "{
    \"resourceType\": \"Parameters\",
    \"parameter\": [{
      \"name\": \"queryResource\",
      \"resource\": {
        \"resourceType\": \"Library\",
        \"status\": \"active\",
        \"type\": {\"coding\": [{\"system\": \"https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes\", \"code\": \"sql-query\"}]},
        \"content\": [{
          \"contentType\": \"application/sql\",
          \"data\": \"${SQL_B64}\"
        }],
        \"relatedArtifact\": [
          {\"type\": \"depends-on\", \"label\": \"conditions\", \"resource\": \"ViewDefinition/<CONDITION_VD_ID>\"}
        ]
      }
    }]
  }"
```

Expected output:

```
condition_name,patient_count
Diabetes mellitus,2
Hypertension,2
Asthma,2
```

### Aggregate: condition count per patient

SQL:

```sql
SELECT p.given_name, p.family_name,
       count(*) AS condition_count
FROM patients p
JOIN conditions c ON concat('Patient/', p.patient_id) = c.patient_ref
GROUP BY p.given_name, p.family_name
ORDER BY condition_count DESC
```

```bash
SQL="SELECT p.given_name, p.family_name, count(*) AS condition_count FROM patients p JOIN conditions c ON concat('Patient/', p.patient_id) = c.patient_ref GROUP BY p.given_name, p.family_name ORDER BY condition_count DESC"
SQL_B64=$(echo -n "$SQL" | base64)

curl -s -X POST "http://localhost:8080/fhir/\$sqlquery-run?_format=csv" \
  -H "Content-Type: application/fhir+json" \
  -d "{
    \"resourceType\": \"Parameters\",
    \"parameter\": [{
      \"name\": \"queryResource\",
      \"resource\": {
        \"resourceType\": \"Library\",
        \"status\": \"active\",
        \"type\": {\"coding\": [{\"system\": \"https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes\", \"code\": \"sql-query\"}]},
        \"content\": [{
          \"contentType\": \"application/sql\",
          \"data\": \"${SQL_B64}\"
        }],
        \"relatedArtifact\": [
          {\"type\": \"depends-on\", \"label\": \"patients\", \"resource\": \"ViewDefinition/<PATIENT_VD_ID>\"},
          {\"type\": \"depends-on\", \"label\": \"conditions\", \"resource\": \"ViewDefinition/<CONDITION_VD_ID>\"}
        ]
      }
    }]
  }"
```

Expected output:

```
given_name,family_name,condition_count
John,Smith,2
Jane,Johnson,1
Alice,Brown,1
Charlie,Davis,1
Bob,Williams,1
```

### Aggregate: patients by age group and condition type

SQL:

```sql
SELECT
  CASE
    WHEN floor(datediff(current_date(), p.birth_date) / 365.25) < 30 THEN '0-29'
    WHEN floor(datediff(current_date(), p.birth_date) / 365.25) < 50 THEN '30-49'
    WHEN floor(datediff(current_date(), p.birth_date) / 365.25) < 70 THEN '50-69'
    ELSE '70+'
  END AS age_group,
  c.condition_name,
  count(DISTINCT p.patient_id) AS patient_count
FROM patients p
JOIN conditions c ON concat('Patient/', p.patient_id) = c.patient_ref
GROUP BY age_group, c.condition_name
ORDER BY age_group, c.condition_name
```

```bash
SQL="SELECT CASE WHEN floor(datediff(current_date(), p.birth_date) / 365.25) < 30 THEN '0-29' WHEN floor(datediff(current_date(), p.birth_date) / 365.25) < 50 THEN '30-49' WHEN floor(datediff(current_date(), p.birth_date) / 365.25) < 70 THEN '50-69' ELSE '70+' END AS age_group, c.condition_name, count(DISTINCT p.patient_id) AS patient_count FROM patients p JOIN conditions c ON concat('Patient/', p.patient_id) = c.patient_ref GROUP BY age_group, c.condition_name ORDER BY age_group, c.condition_name"
SQL_B64=$(echo -n "$SQL" | base64)

curl -s -X POST "http://localhost:8080/fhir/\$sqlquery-run?_format=csv" \
  -H "Content-Type: application/fhir+json" \
  -d "{
    \"resourceType\": \"Parameters\",
    \"parameter\": [{
      \"name\": \"queryResource\",
      \"resource\": {
        \"resourceType\": \"Library\",
        \"status\": \"active\",
        \"type\": {\"coding\": [{\"system\": \"https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes\", \"code\": \"sql-query\"}]},
        \"content\": [{
          \"contentType\": \"application/sql\",
          \"data\": \"${SQL_B64}\"
        }],
        \"relatedArtifact\": [
          {\"type\": \"depends-on\", \"label\": \"patients\", \"resource\": \"ViewDefinition/<PATIENT_VD_ID>\"},
          {\"type\": \"depends-on\", \"label\": \"conditions\", \"resource\": \"ViewDefinition/<CONDITION_VD_ID>\"}
        ]
      }
    }]
  }"
```

### Simple query against a single view (NDJSON output)

```bash
SQL="SELECT patient_id, given_name, family_name FROM patients LIMIT 3"
SQL_B64=$(echo -n "$SQL" | base64)

curl -s -X POST "http://localhost:8080/fhir/\$sqlquery-run" \
  -H "Content-Type: application/fhir+json" \
  -H "Accept: application/x-ndjson" \
  -d "{
    \"resourceType\": \"Parameters\",
    \"parameter\": [{
      \"name\": \"queryResource\",
      \"resource\": {
        \"resourceType\": \"Library\",
        \"status\": \"active\",
        \"type\": {\"coding\": [{\"system\": \"https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes\", \"code\": \"sql-query\"}]},
        \"content\": [{
          \"contentType\": \"application/sql\",
          \"data\": \"${SQL_B64}\"
        }],
        \"relatedArtifact\": [
          {\"type\": \"depends-on\", \"label\": \"patients\", \"resource\": \"ViewDefinition/<PATIENT_VD_ID>\"}
        ]
      }
    }]
  }"
```

### JSON output format

```bash
SQL="SELECT condition_name, onset_date FROM conditions ORDER BY onset_date"
SQL_B64=$(echo -n "$SQL" | base64)

curl -s -X POST "http://localhost:8080/fhir/\$sqlquery-run?_format=json" \
  -H "Content-Type: application/fhir+json" \
  -d "{
    \"resourceType\": \"Parameters\",
    \"parameter\": [{
      \"name\": \"queryResource\",
      \"resource\": {
        \"resourceType\": \"Library\",
        \"status\": \"active\",
        \"type\": {\"coding\": [{\"system\": \"https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes\", \"code\": \"sql-query\"}]},
        \"content\": [{
          \"contentType\": \"application/sql\",
          \"data\": \"${SQL_B64}\"
        }],
        \"relatedArtifact\": [
          {\"type\": \"depends-on\", \"label\": \"conditions\", \"resource\": \"ViewDefinition/<CONDITION_VD_ID>\"}
        ]
      }
    }]
  }"
```

### SQL validation: DDL/DML rejected

```bash
SQL="DROP TABLE conditions"
SQL_B64=$(echo -n "$SQL" | base64)

curl -s -X POST "http://localhost:8080/fhir/\$sqlquery-run" \
  -H "Content-Type: application/fhir+json" \
  -d "{
    \"resourceType\": \"Parameters\",
    \"parameter\": [{
      \"name\": \"queryResource\",
      \"resource\": {
        \"resourceType\": \"Library\",
        \"status\": \"active\",
        \"type\": {\"coding\": [{\"system\": \"https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes\", \"code\": \"sql-query\"}]},
        \"content\": [{
          \"contentType\": \"application/sql\",
          \"data\": \"${SQL_B64}\"
        }],
        \"relatedArtifact\": []
      }
    }]
  }"
```

Expected: 400 error with `"SQL contains a disallowed operation"`.
