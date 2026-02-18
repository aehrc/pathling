# Copyright © 2026, Commonwealth Scientific and Industrial Research Organisation (CSIRO)
# ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.

FROM eclipse-temurin:21-jre AS base

# Install Python and pip.
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files and install.
COPY pyproject.toml .
COPY src/ src/
RUN python3 -m venv /app/.venv && \
    /app/.venv/bin/pip install --no-cache-dir .

ENV PATH="/app/.venv/bin:$PATH"

# This caches the download of the Spark dependencies at build time.
RUN python3 -c "from pathling import PathlingContext; pc = PathlingContext.create(); pc.spark.stop()"

ENV PORT=8080

EXPOSE 8080

CMD ["python3", "-m", "fhirpath_lab_api"]
