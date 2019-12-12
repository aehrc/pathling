#!/bin/bash
set -e

BASE_DIR="$(cd `dirname $0`/.. && pwd)" 
PATHLING_URL=$(terraform output -state="${BASE_DIR}/terraform/terraform.tfstate" pathling_fhir_url)

curl -v -H "Content-Type: application/json"  "${PATHLING_URL}/metadata"
echo
