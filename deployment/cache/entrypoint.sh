#!/bin/bash
# Copyright © 2025, Commonwealth Scientific and Industrial Research Organisation (CSIRO)
# ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.
set -e

envs=`printenv`

for env in $envs
do
    IFS== read name value <<< "$env"
    sed -i "s|\${${name}}|${value}|g" /etc/varnish/default.vcl
done

source /usr/local/bin/docker-varnish-entrypoint
