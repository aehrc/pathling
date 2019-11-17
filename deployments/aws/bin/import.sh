#!/bin/bash
set -e

BASE_DIR="$(cd `dirname $0`/.. && pwd)"
CLINSIGHT_URL=$(terraform output -state="${BASE_DIR}/terraform/terraform.tfstate" clinsight_fhir_url)
DATASET_URL="s3://aehrc-clinsight/share/data/test-set"

(cat | curl -v -d "@-" -H "Content-Type: application/json" -X POST "${CLINSIGHT_URL}/\$import")  << EOF
{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "AllergyIntolerance"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/AllergyIntolerance.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "CarePlan"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/CarePlan.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Claim"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Claim.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Condition"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Condition.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "DiagnosticReport"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/DiagnosticReport.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Encounter"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Encounter.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "ExplanationOfBenefit"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/ExplanationOfBenefit.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Goal"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Goal.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "ImagingStudy"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/ImagingStudy.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Immunization"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Immunization.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "MedicationRequest"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/MedicationRequest.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Observation"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Observation.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Organization"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Organization.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Patient"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Patient.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Practitioner"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Practitioner.ndjson"
        }
      ]
    },
    {
      "name": "source",
      "part": [
        {
          "name": "resourceType",
          "valueCode": "Procedure"
        },
        {
          "name": "url",
          "valueUrl": "${DATASET_URL}/Procedure.ndjson"
        }
      ]
    }
  ]
}
EOF

echo


