#!/usr/bin/env bash

rm -rf dist/*
yarn webpack --mode=production
docker build . -t docker-registry.it.csiro.au/clinsight/ui --build-arg version=$(git rev-parse HEAD)
