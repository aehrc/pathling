#!/bin/bash
set -e

BASE_DIR="$(cd `dirname $0`/.. && pwd)" 
CLINSIGHT_URL=$(terraform output -state="${BASE_DIR}/terraform/terraform.tfstate" clinsight_fhir_url)

curl -v -H "Content-Type: application/json"  "${CLINSIGHT_URL}/metadata"
echo
