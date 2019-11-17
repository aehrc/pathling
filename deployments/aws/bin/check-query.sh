#!/bin/bash
set -e

BASE_DIR="$(cd `dirname $0`/.. && pwd)"
CLINSIGHT_URL=$(terraform output -state="${BASE_DIR}/terraform/terraform.tfstate" clinsight_fhir_url)

(cat | curl -v -d "@-" -H "Content-Type: application/json" -X POST "${CLINSIGHT_URL}/\$aggregate")  << EOF
{
    "resourceType": "Parameters",
    "parameter": [
      {
        "name": "subjectResource",
        "valueCode": "Patient"
      },
        {
            "name": "aggregation",
            "part": [
                {
                    "name": "label",
                    "valueString": "Number of patients"
                },
                {
                    "name": "expression",
                    "valueString": "count()"
                }
            ]
        },
        {
            "name": "grouping",
            "part": [
                {
                    "name": "label",
                    "valueString": "Gender"
                },
                {
                    "name": "expression",
                    "valueString": "gender"
                }
            ]
        },
        {
          "name": "filter",
          "valueString": "gender = 'female'"
        }
    ]
}
EOF

echo


