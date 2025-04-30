---
sidebar_position: 2
description: Instructions for getting started with Pathling server.
---

# Getting started

The easiest way to get Pathling up and running is to use the pre-built
[Docker](https://www.docker.com/) image.

Instructions for installing Docker on your machine can be found here:

- [Install Docker Desktop on Mac](https://docs.docker.com/docker-for-mac/install/)
- [Install Docker Desktop on Windows](https://docs.docker.com/docker-for-windows/install/)
- [Install Docker for Linux](https://docs.docker.com/install/linux/ubuntu/)

Once you have Docker installed on your computer, you can run an instance of
Pathling by issuing the following command:

```
docker run --rm -p 8080:8080 aehrc/pathling
```

This will download and run a fully functional instance of Pathling, with
sensible default configuration. Any data you load into this instance will not be
persisted beyond the termination of the container, as you have not yet set up a
persistent volume for storage.

The `--rm` option deletes the container after it has been stopped.

The `-p 8080:8080` option maps the internal port 8080 (which is the port that
Pathling runs on by default) to port 8080 on the host machine.

The `aehrc/pathling` image reference points to the latest image of
[Pathling on Docker Hub](https://hub.docker.com/r/aehrc/pathling). Tags can be
added to the end of the image reference to refer to particular versions of
Pathling.

## Using Docker Compose

[Docker Compose](https://docs.docker.com/compose/) is a tool for running a
container (or set of containers) using a configuration defined in a
[YAML](https://yaml.org/) file (known as a
[Docker Compose file](https://docs.docker.com/compose/compose-file/)).

You can find instructions for installing Docker Compose at:
[Install Compose](https://docs.docker.com/compose/install/)

As an example of using Pathling with Docker Compose, we will create a simple
Docker Compose file that sets up persistent storage, in addition to overriding
several configuration parameters using environment variables.

```yaml
version: "3"
services:
  pathling:
    image: aehrc/pathling:7
    ports:
      - 8080:8080
    environment:
      pathling.terminology.serverUrl: https://tx.somecompany.com/fhir
      pathling.cors.allowedOrigins: http://localhost:3000
    volumes:
      - pathling:/usr/share/warehouse
      - /home/me/data:/usr/share/staging
volumes:
  pathling:
    driver: local
```

This file should be saved into a file named `docker-compose.yml` in the current
directory. Then the following command can be issued to run the Pathling server:

```
docker-compose up
```

In this example, the `image` key is being used to refer to the latest `7.x`
version of the Pathling image on Docker Hub.

We are setting the `pathling.terminology.serverUrl` configuration variable to
point to a
particular [FHIR terminology service](https://hl7.org/fhir/R4/terminology-service.html)
endpoint.

The `pathling.cors.allowedOrigins` variable is set to
`http://localhost:3000`, which will configure the CORS support in Pathling to
allow a browser-based frontend application to integrate with the Pathling API.

For a full list of all available configuration variables, see
[Configuration](./configuration).

In the `volumes` section, we set up two volumes: one for storing the warehouse
data files, and one for importing data. The first volume, named `pathling`, is
persistent but managed by Docker. The second volume is a "bind mount", where we
are mounting an existing directory on our host operating system into the Docker
container. This might be where we have our FHIR data stored. For more
information, read
[Choose the right type of mount](https://docs.docker.com/storage/#choose-the-right-type-of-mount)
.

Now when we invoke the [import operation](./operations/import), we can
point Pathling to a filesystem URL,
e.g. `file:///usr/share/staging/Patient.ndjson`. Pathling will persist this data
within files that it stores in the `pathling` managed volume.
