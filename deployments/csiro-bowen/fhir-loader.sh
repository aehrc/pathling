#!/usr/bin/env bash

rsync -avz --delete master/fhir-loader hb-15-cdc001.it.csiro.au:/opt/docker/pathling
docker push docker-registry.it.csiro.au/pathling/fhir-loader
ssh hb-15-cdc001.it.csiro.au "cd /opt/docker/pathling/fhir-loader && docker-compose pull && docker-compose up -d && docker-compose logs -f"