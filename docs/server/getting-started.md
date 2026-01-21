# Getting started

This guide will help you get Pathling Server running quickly using Docker.

## Docker quickstart[​](#docker-quickstart "Direct link to Docker quickstart")

The simplest way to run Pathling Server is with Docker:

```
docker run -p 8080:8080 -p 8081:8081 ghcr.io/aehrc/pathling:latest
```

Once started, you can access:

* **FHIR API**: <http://localhost:8080/fhir>
* **Admin UI**: <http://localhost:8080/admin/>
* **Health check**: <http://localhost:8081/actuator/health>

## Docker Compose[​](#docker-compose "Direct link to Docker Compose")

For persistent storage and custom configuration, use Docker Compose:

```
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

```
docker compose up
```

## Next steps[​](#next-steps "Direct link to Next steps")

* [Operations](/docs/server/operations) - Learn about the FHIR operations available
* [Admin UI](/docs/server/admin-ui.md) - Visual interface for managing data and testing queries
* [Configuration](/docs/server/configuration.md) - Customise server behaviour with environment variables
* [Kubernetes deployment](/docs/server/deployment/kubernetes.md) - Deploy to a Kubernetes cluster
