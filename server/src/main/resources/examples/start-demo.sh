#!/usr/bin/env bash
#
# Starts the Pathling server with a temporary warehouse and loads test data
# for demonstrating the $viewdefinition-run and $sqlquery-run operations.
#
# Usage (from any directory within the repository):
#   ./server/src/main/resources/examples/start-demo.sh
#
# The script will print the ViewDefinition IDs needed for the $sqlquery-run
# examples in sqlquery-run-examples.md.

set -euo pipefail

BASE_URL="http://localhost:8080/fhir"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Resolve the server module directory (four levels up from examples/).
SERVER_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
cd "$SERVER_DIR"
echo "Working directory: $(pwd)"

# ── helpers ──────────────────────────────────────────────────────────────────

wait_for_server() {
  echo "Waiting for server..."
  for i in $(seq 1 40); do
    if curl -sf -o /dev/null "$BASE_URL/metadata" 2>/dev/null; then
      echo "Server is ready."
      return 0
    fi
    sleep 2
  done
  echo "ERROR: Server did not start within 80 seconds." >&2
  exit 1
}

post_resource() {
  local type="$1" body="$2"
  curl -s -X POST "$BASE_URL/$type" \
    -H "Content-Type: application/fhir+json" \
    -d "$body"
}

# ── generate NDJSON test data ────────────────────────────────────────────────

DATA_DIR="$(mktemp -d)"
echo "Generating test data in $DATA_DIR ..."

python3 << PYEOF
import json, random

DATA_DIR = "$DATA_DIR"
PATIENT_COUNT = 1000
random.seed(42)

FAMILIES = [
    "Smith", "Johnson", "Williams", "Brown", "Davis", "Miller", "Wilson",
    "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris",
    "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark",
    "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young",
    "King", "Wright", "Scott", "Green", "Baker", "Adams", "Nelson",
    "Hill", "Ramirez", "Campbell", "Mitchell", "Roberts", "Carter",
]
GIVENS_MALE = [
    "James", "John", "Robert", "Michael", "David", "William", "Richard",
    "Joseph", "Thomas", "Charles", "Daniel", "Matthew", "Anthony", "Mark",
    "Steven", "Paul", "Andrew", "Joshua", "Kenneth", "Kevin",
]
GIVENS_FEMALE = [
    "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth",
    "Susan", "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty",
    "Margaret", "Sandra", "Ashley", "Emily", "Donna", "Michelle", "Carol",
]
CONDITIONS = [
    {"code": "73211009", "display": "Diabetes mellitus"},
    {"code": "38341003", "display": "Hypertension"},
    {"code": "195967001", "display": "Asthma"},
]
# Every 5 patients: pat0 gets 2 conditions, the rest get 1 each.
CONDITION_PATTERN = [
    [CONDITIONS[0], CONDITIONS[1]],
    [CONDITIONS[2]],
    [CONDITIONS[0]],
    [CONDITIONS[1]],
    [CONDITIONS[2]],
]

def random_date(lo, hi):
    return f"{random.randint(lo,hi):04d}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

cond_idx = 0
with open(f"{DATA_DIR}/Patient.ndjson", "w") as pf, \
     open(f"{DATA_DIR}/Condition.ndjson", "w") as cf:
    for i in range(PATIENT_COUNT):
        pid = f"pat-{i+1:04d}"
        gender = "male" if i % 2 == 0 else "female"
        givens = GIVENS_MALE if gender == "male" else GIVENS_FEMALE
        pf.write(json.dumps({
            "resourceType": "Patient", "id": pid, "gender": gender,
            "birthDate": random_date(1950, 2005),
            "name": [{"use": "official",
                       "family": FAMILIES[i % len(FAMILIES)],
                       "given": [givens[i % len(givens)]]}],
        }) + "\n")
        for cond in CONDITION_PATTERN[i % len(CONDITION_PATTERN)]:
            cond_idx += 1
            cid = f"cnd-{cond_idx:04d}"
            cf.write(json.dumps({
                "resourceType": "Condition", "id": cid,
                "subject": {"reference": f"Patient/{pid}"},
                "code": {"coding": [{"system": "http://snomed.info/sct",
                                      "code": cond["code"],
                                      "display": cond["display"]}]},
                "onsetDateTime": random_date(2005, 2025),
            }) + "\n")

print(f"  Generated {PATIENT_COUNT} patients, {cond_idx} conditions")
PYEOF

echo "  Patient.ndjson:   $(wc -l < "$DATA_DIR/Patient.ndjson") lines"
echo "  Condition.ndjson: $(wc -l < "$DATA_DIR/Condition.ndjson") lines"

# ── start server ─────────────────────────────────────────────────────────────

WAREHOUSE_DIR="$(mktemp -d)/warehouse"
mkdir -p "$WAREHOUSE_DIR"
echo ""
echo "Warehouse: $WAREHOUSE_DIR"

export PATHLING_STORAGE_WAREHOUSEURL="file://$WAREHOUSE_DIR"
export PATHLING_STORAGE_DATABASENAME="default"
export PATHLING_IMPORT_ALLOWABLESOURCES="file://$DATA_DIR"

echo "Starting Pathling server..."
mvn -q spring-boot:run \
  -Dspring-boot.run.jvmArguments="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED -XX:ReservedCodeCacheSize=256m" \
  &
SERVER_PID=$!
trap 'echo "Stopping server (PID $SERVER_PID)..."; kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null; rm -rf "$DATA_DIR" "$(dirname "$WAREHOUSE_DIR")"' EXIT

wait_for_server

# ── bulk import via $import ──────────────────────────────────────────────────

echo ""
echo "=== Importing test data via \$import ==="

IMPORT_RESPONSE=$(curl -s -D- -X POST "$BASE_URL/\$import" \
  -H "Content-Type: application/json" \
  -H "Accept: application/fhir+json" \
  -H "Prefer: respond-async" \
  -d "{
    \"inputFormat\": \"application/fhir+ndjson\",
    \"inputSource\": \"file://$DATA_DIR\",
    \"input\": [
      {\"type\": \"Patient\", \"url\": \"file://$DATA_DIR/Patient.ndjson\"},
      {\"type\": \"Condition\", \"url\": \"file://$DATA_DIR/Condition.ndjson\"}
    ]
  }")

JOB_URL=$(echo "$IMPORT_RESPONSE" | grep -i "^Content-Location:" | tr -d '\r' | awk '{print $2}')
if [ -z "$JOB_URL" ]; then
  echo "ERROR: Import request failed." >&2
  echo "$IMPORT_RESPONSE"
  exit 1
fi
echo "  Import job: $JOB_URL"

# Poll the job until it completes.
echo "  Waiting for import to complete..."
for i in $(seq 1 60); do
  sleep 2
  JOB_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$JOB_URL" -H "Accept: application/fhir+json")
  if [ "$JOB_STATUS" = "200" ]; then
    echo "  Import complete."
    break
  elif [ "$JOB_STATUS" = "202" ]; then
    # Still processing.
    :
  else
    echo "  Import failed (HTTP $JOB_STATUS):"
    curl -s "$JOB_URL" -H "Accept: application/fhir+json"
    exit 1
  fi
done

# ── create view definitions ──────────────────────────────────────────────────

echo ""
echo "=== Creating ViewDefinitions ==="

PATIENT_VD=$(post_resource ViewDefinition '{
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
}')
PATIENT_VD_ID=$(echo "$PATIENT_VD" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id','UNKNOWN'))")
echo "  patients ViewDefinition: $PATIENT_VD_ID"

CONDITION_VD=$(post_resource ViewDefinition '{
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
}')
CONDITION_VD_ID=$(echo "$CONDITION_VD" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id','UNKNOWN'))")
echo "  conditions ViewDefinition: $CONDITION_VD_ID"

# ── summary ──────────────────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "  Demo server ready at $BASE_URL"
echo ""
echo "  Patients ViewDefinition ID:   $PATIENT_VD_ID"
echo "  Conditions ViewDefinition ID: $CONDITION_VD_ID"
echo ""
echo "  Replace <PATIENT_VD_ID> and <CONDITION_VD_ID> in the"
echo "  examples in sqlquery-run-examples.md with the IDs above."
echo ""
echo "  Press Ctrl+C to stop the server."
echo "============================================================"
echo ""

# Keep the script alive until the user kills it.
wait $SERVER_PID
