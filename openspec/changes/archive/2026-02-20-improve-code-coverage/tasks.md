## 1. Phase 1 — Simple classes (0-50% coverage, few dependencies)

- [x] 1.1 Add tests for `SecurityError` (constructors, message propagation)
- [x] 1.2 Add tests for `RequestTag` and `SubmissionMetadata` (data classes)
- [x] 1.3 Add tests for `SubmissionRegistry` (put, get, updateState, remove, getBySubmitter, getByJobId, addManifestJob, updateManifestJob)
- [x] 1.4 Add tests for `UpdateProvider` (delegation to UpdateExecutor)
- [x] 1.5 Add tests for `PathlingAuthenticationConverter` (authorities converter configuration)
- [x] 1.6 Add tests for `CreateProviderFactory`, `DeleteProviderFactory`, `UpdateProviderFactory`, `SearchProviderFactory` (provider creation per resource type)
- [x] 1.7 Run JaCoCo locally and verify coverage improvement from Phase 1

## 2. Phase 2 — Medium-complexity classes (5-30% coverage, HTTP/servlet mocking)

- [x] 2.1 Add tests for `ImportManifestInterceptor` (JSON-to-Parameters conversion, non-JSON pass-through, error handling)
- [x] 2.2 Add tests for `OidcDiscoveryFetcher` (successful fetch, HTTP errors, malformed JSON) using WireMock
- [x] 2.3 Add tests for `ExportOperationHelper` (export parameter construction and validation)
- [x] 2.4 Add tests for `SparkJobListener` (stage completion tracking, job progress updates)
- [x] 2.5 Add tests for `Spark` (session configuration, property resolution)
- [x] 2.6 Add tests for `SubmitterConfiguration` (configuration binding and validation)
- [x] 2.7 Add tests for `ErrorReportingInterceptor` (error report generation)
- [x] 2.8 Add tests for `ImportLock` (lock acquisition and release)
- [x] 2.9 Run JaCoCo locally and verify cumulative coverage improvement

## 3. Phase 3 — Complex classes (7-25% coverage, deep framework integration)

- [x] 3.1 Add tests for `ViewDefinitionExportProvider` (parameter validation, view extraction, response generation)
- [x] 3.2 Add tests for `ImportProvider` (Parameters parsing, async job creation, validation)
- [x] 3.3 Add tests for `ImportPnpProvider` (remote manifest fetch, job coordination)
- [x] 3.4 Add tests for `ImportPnpExecutor` (data fetch execution, failure recovery)
- [x] 3.5 Add tests for `PatientExportProvider` and `GroupExportProvider` (compartment-scoped export)
- [x] 3.6 Add tests for `PathlingJwtDecoderBuilder` (decoder construction from OIDC config, key selection)
- [x] 3.7 Add tests for `BulkSubmitAuthProvider` (authentication and authorisation logic)
- [x] 3.8 Add tests for `FhirServer` (provider registration, interceptor registration)
- [x] 3.9 Run JaCoCo locally and verify overall coverage is at least 80%
