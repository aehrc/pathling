## Why

The FHIRPath Lab API container runs Flask's built-in werkzeug development server
in production. Werkzeug itself logs a warning about this: "This is a development
server. Do not use it in a production deployment. Use a production WSGI server
instead." The development server is single-threaded, has no worker management,
and is not designed for production workloads.

## What changes

- Add Gunicorn as a production WSGI server dependency.
- Update the container entrypoint to start the application via Gunicorn instead
  of `python3 -m fhirpath_lab_api`.
- Remove the `app.run()` call from the `__main__.py` module, replacing it with
  a Gunicorn-compatible entry point.
- Make worker count and bind address configurable via environment variables.

## Capabilities

### New capabilities

_None._

### Modified capabilities

- `fhirpath-lab-server`: The server process management changes from Flask's
  development server to Gunicorn. The WSGI application entry point must be
  exposed for Gunicorn to import.

## Impact

- `fhirpath-lab-api/pyproject.toml` - New `gunicorn` dependency.
- `fhirpath-lab-api/src/fhirpath_lab_api/__main__.py` - Entry point changes.
- `fhirpath-lab-api/Dockerfile` - Updated CMD to use Gunicorn.
- `fhirpath-lab-api/uv.lock` - Lockfile update.
