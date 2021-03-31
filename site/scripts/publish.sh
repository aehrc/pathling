#!/usr/bin/env bash

aws s3 sync ../_site $1 --storage-class REDUCED_REDUNDANCY \
    --cache-control public,max-age=86400