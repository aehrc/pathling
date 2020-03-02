---
layout: page
title: Known issues
nav_order: 7
parent: Documentation
---

# Known issues

1. [aggregate](./aggregate.html) queries with multiple aggregation expressions
   which return multiple elements per resource currently return incorrect
   results. The workaround for this issue is to simply use separate queries to
   calculate each aggregation.
