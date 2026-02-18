## ADDED Requirements

### Requirement: Production WSGI server

The server SHALL use Gunicorn as the production WSGI server when running inside
the Docker container. The container CMD SHALL invoke Gunicorn directly rather
than using Flask's built-in development server.

#### Scenario: Container starts with Gunicorn

- **WHEN** the Docker container starts with default configuration
- **THEN** the process running inside the container is Gunicorn serving the
  Flask application, and the werkzeug development server warning does not appear
  in the logs

#### Scenario: Configurable bind address

- **WHEN** the `PORT` environment variable is set to `9090`
- **THEN** Gunicorn binds to `0.0.0.0:9090`

#### Scenario: Default port

- **WHEN** the `PORT` environment variable is not set
- **THEN** Gunicorn binds to `0.0.0.0:8080`

#### Scenario: Configurable worker count

- **WHEN** the `WEB_WORKERS` environment variable is set to `2`
- **THEN** Gunicorn starts with 2 worker processes

#### Scenario: Default worker count

- **WHEN** the `WEB_WORKERS` environment variable is not set
- **THEN** Gunicorn starts with 1 worker process

### Requirement: Local development entry point

The `python -m fhirpath_lab_api` entry point SHALL remain functional for local
development, using Flask's built-in server.

#### Scenario: Local development server

- **WHEN** the application is started via `python -m fhirpath_lab_api`
- **THEN** the Flask development server starts on the configured port
