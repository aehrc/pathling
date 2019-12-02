#!/bin/bash
set -e

BASE_DIR="$(cd `dirname $0`/.. && pwd)" 
MASTER_DNS=$(terraform output -state="${BASE_DIR}/terraform/terraform.tfstate" emr_cluster_master_dns)

ssh -i ~/.ssh/clinsight.pem hadoop@${MASTER_DNS} "$@"
