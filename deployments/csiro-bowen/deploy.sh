#!/usr/bin/env bash

rsync -avz master/ hb-15-cdc001.it.csiro.au:/opt/docker/clinsight
rsync -avz worker/ hb-15-cdc002.it.csiro.au:/opt/docker/clinsight

ssh hb-15-cdc001.it.csiro.au "cd /opt/docker/clinsight && docker-compose pull && docker-compose up -d"
ssh hb-15-cdc001.it.csiro.au "docker images | grep \"<none>\" | awk '{print $3}' | xargs docker rmi"

ssh hb-15-cdc002.it.csiro.au "cd /opt/docker/clinsight && docker-compose pull && docker-compose up -d"
ssh hb-15-cdc002.it.csiro.au "docker images | grep \"<none>\" | awk '{print $3}' | xargs docker rmi"
