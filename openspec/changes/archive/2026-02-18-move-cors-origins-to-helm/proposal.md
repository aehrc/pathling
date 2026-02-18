## Why

The FHIRPath Lab API server hardcodes default CORS origins in the Python
application code. This couples deployment-specific configuration to the
application, making it harder to manage origins across environments. Moving CORS
origins entirely to the Helm chart values makes the application simpler and
the deployment more transparent.

## What changes

- Remove `DEFAULT_CORS_ORIGINS` from `app.py`; CORS origins are now driven
  entirely by the `CORS_ALLOWED_ORIGINS` environment variable.
- Set `CORS_ALLOWED_ORIGINS` in the Helm chart `values.yaml` with the
  production defaults: `https://fhirpath-lab.azurewebsites.net` and
  `http://localhost:3000`.
- Update tests and documentation to reflect the new behaviour.

## Capabilities

### New capabilities

(none)

### Modified capabilities

- `fhirpath-lab-server`: CORS origins are no longer hardcoded; the server reads
  all origins from the `CORS_ALLOWED_ORIGINS` environment variable.
- `fhirpath-lab-deployment`: The Helm chart provides default CORS origins via
  the `config` values.

## Impact

- `fhirpath-lab-api/src/fhirpath_lab_api/app.py`: Remove default origins list,
  simplify CORS setup.
- `fhirpath-lab-api/tests/test_app.py`: Update CORS tests.
- `fhirpath-lab-api/README.md`: Update configuration docs.
- `deployment/fhirpath-lab-api/chart/values.yaml`: Add default CORS origins.
- `deployment/fhirpath-lab-api/chart/README.md`: Update configuration docs.
