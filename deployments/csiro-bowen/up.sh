#!/usr/bin/env bash

ssh hb-15-cdc00$1.it.csiro.au "cd /opt/docker/clinsight && docker-compose -p clinsight up -d && docker-compose logs -f"
