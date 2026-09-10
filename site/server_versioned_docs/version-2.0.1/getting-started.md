---
sidebar_position: 2
sidebar_label: Getting started
description: Quick start guide for running Pathling Server with Docker.
---

# Getting started

This guide will help you get Pathling Server running quickly using Docker.

## Docker quickstart

The simplest way to run Pathling Server is with Docker:

```bash
docker run -p 8080:8080 -p 8081:8081 ghcr.io/aehrc/pathling:latest
```

Once started, you can access:

- **FHIR API**: http://localhost:8080/fhir
- **Admin UI**: http://localhost:8080/admin/
- **Health check**: http://localhost:8081/actuator/health

## Docker Compose

For persistent storage and custom configuration, use Docker Compose:

```yaml
services:
    pathling:
        image: ghcr.io/aehrc/pathling:latest
        ports:
            - "8080:8080"
            - "8081:8081"
        volumes:
            - pathling-data:/usr/share/warehouse
        environment:
            - pathling.storage.warehouseUrl=file:///usr/share/warehouse
            - pathling.terminology.serverUrl=https://tx.ontoserver.csiro.au/fhir

volumes:
    pathling-data:
```

Save this as `docker-compose.yml` and run:

```bash
docker compose up
```
