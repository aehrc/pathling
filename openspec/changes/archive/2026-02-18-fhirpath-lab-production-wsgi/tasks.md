## 1. Dependencies

- [x] 1.1 Add `gunicorn` to `pyproject.toml` dependencies
- [x] 1.2 Regenerate `uv.lock`

## 2. Application entry point

- [x] 2.1 Create a module-level WSGI app object in `app.py` (or a dedicated module) that Gunicorn can import
- [x] 2.2 Keep `__main__.py` functional for local development

## 3. Container configuration

- [x] 3.1 Update Dockerfile CMD to invoke Gunicorn with configurable port and worker count via environment variables

## 4. Verification

- [x] 4.1 Build the Docker image and verify Gunicorn starts without the werkzeug warning
- [x] 4.2 Verify the `/healthcheck` and `/$fhirpath-r4` endpoints still work
