#!/usr/bin/env bash

ssh hb-15-cdc00$1.it.csiro.au "cd /opt/docker/pathling && docker-compose -p pathling up -d && docker-compose logs -f"
