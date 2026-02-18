## 1. Application code

- [x] 1.1 Remove `DEFAULT_CORS_ORIGINS` from `app.py` and simplify CORS setup to read only from `CORS_ALLOWED_ORIGINS`
- [x] 1.2 Update CORS tests in `test_app.py` to set `CORS_ALLOWED_ORIGINS` via environment and test the no-origins case

## 2. Helm chart

- [x] 2.1 Add `CORS_ALLOWED_ORIGINS` to `values.yaml` with default value `https://fhirpath-lab.azurewebsites.net,http://localhost:3000`

## 3. Documentation

- [x] 3.1 Update `fhirpath-lab-api/README.md` to reflect that CORS origins are configured entirely via environment variable
- [x] 3.2 Update `deployment/fhirpath-lab-api/chart/README.md` to document the default CORS origins in the chart
