## Context

The FHIRPath Lab API is a Flask application that currently starts via
`python3 -m fhirpath_lab_api`, which calls `app.run()` using Flask's built-in
werkzeug development server. This server is single-threaded, has no worker
process management, and logs a warning on every startup advising against
production use.

The application runs in a Docker container deployed to Kubernetes, where it
serves real traffic from the FHIRPath Lab frontend.

## Goals / Non-goals

**Goals:**

- Serve the Flask application via a production-grade WSGI server.
- Eliminate the werkzeug development server warning from container logs.
- Maintain configurability of bind address and port via environment variables.

**Non-goals:**

- Async/ASGI migration (the application is synchronous and CPU-bound via Spark).
- Multi-worker configuration. The application creates a single PathlingContext
  (Spark session) at import time, and Spark is not designed to run multiple
  driver sessions in one container. A single Gunicorn worker is appropriate.

## Decisions

### Use Gunicorn as the WSGI server

Gunicorn is the standard production WSGI server for Python applications. It is
mature, well-documented, and has minimal configuration overhead. Alternatives
considered:

- **uWSGI**: More complex configuration, heavier dependency. Unnecessary for a
  single-worker deployment.
- **Waitress**: Cross-platform but less common in Linux container deployments.

Gunicorn is the simplest choice that eliminates the development server warning
and provides production-grade request handling.

### Single worker with configurable count

Default to 1 worker because the PathlingContext creates a Spark driver session
that is not designed for concurrent instances within one process tree. Expose
`WEB_WORKERS` as an environment variable so the deployment can be adjusted if
needed in future.

### Preserve `__main__.py` for local development

Keep `__main__.py` functional for local development (`python -m fhirpath_lab_api`
still works), but change the container CMD to invoke Gunicorn directly. This
avoids breaking the local development workflow.

## Risks / Trade-offs

- **Single worker limitation**: With one Gunicorn worker, the server can only
  handle one request at a time. This matches current behaviour with the
  development server and is acceptable because FHIRPath evaluation is CPU-bound
  via Spark. Horizontal scaling is handled at the Kubernetes level.
