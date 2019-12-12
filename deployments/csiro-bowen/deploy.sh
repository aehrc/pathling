#!/usr/bin/env bash

echo "Pushing docker images..."
source ../../.local/push-docker.sh
set +e
echo

echo "Syncing Docker Compose files..."
rsync -avz --delete master/ hb-15-cdc001.it.csiro.au:/opt/docker/pathling
rsync -avz --delete worker/ hb-15-cdc002.it.csiro.au:/opt/docker/pathling
echo

echo "Updating stack on hb-15-cdc001.it.csiro.au..."
ssh hb-15-cdc001.it.csiro.au "cd /opt/docker/pathling && docker-compose pull && docker-compose up -d"
ssh hb-15-cdc001.it.csiro.au "docker images | grep \"<none>\" | awk '{print $3}' | xargs docker rmi"
echo

echo "Updating stack on hb-15-cdc002.it.csiro.au..."
ssh hb-15-cdc002.it.csiro.au "cd /opt/docker/pathling && docker-compose pull && docker-compose up -d"
ssh hb-15-cdc002.it.csiro.au "docker images | grep \"<none>\" | awk '{print $3}' | xargs docker rmi"
echo
