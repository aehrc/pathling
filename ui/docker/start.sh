#!/usr/bin/env bash
set -e

# Build a configuration file from environment variables.
/buildConfig.sh >/usr/share/nginx/html/config.json

exec nginx -g 'daemon off;'
