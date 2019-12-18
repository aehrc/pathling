---
layout: page
title: Multi-tenancy
nav_order: 1
parent: Roadmap
grand_parent: Documentation
---

# Multi-tenancy

This change will allow for the hosting of multiple databases by a single
Pathling server.

Each database will expose its own FHIR endpoint, for example:

```
myserver.com/database1/fhir
myserver.com/database2/fhir
```

The [import](../import.html) operation will be modified to allow for the
selection of the database that data is to be imported to.

Next: [Select search parameter](./select.html)
