## ADDED Requirements

### Requirement: Phase 1 unit tests for simple classes

Unit tests SHALL be added for server classes that have few dependencies and
straightforward logic. These classes currently have 0-50% coverage and can be
tested with minimal mocking.

Target files:

- `SecurityError.java` (0%, 4 lines)
- `SubmissionRegistry.java` (5.7%, 49 lines)
- `UpdateProvider.java` (0%, 17 lines)
- `PathlingAuthenticationConverter.java` (0%, 9 lines)
- `CreateProviderFactory.java` (45.5%, 11 lines)
- `DeleteProviderFactory.java` (45.5%, 11 lines)
- `UpdateProviderFactory.java` (45.5%, 11 lines)
- `SearchProviderFactory.java` (53.8%, 13 lines)
- `RequestTag.java` (0%, 2 lines)
- `SubmissionMetadata.java` (0%, 1 line)

#### Scenario: SecurityError construction and message propagation

- **WHEN** a SecurityError is constructed with a message
- **THEN** the message SHALL be retrievable via `getMessage()`

#### Scenario: SubmissionRegistry stores and retrieves submissions

- **WHEN** a Submission is added to the SubmissionRegistry
- **THEN** it SHALL be retrievable by its identifier

#### Scenario: SubmissionRegistry updates submission state

- **WHEN** a submission's state is updated in the registry
- **THEN** subsequent retrievals SHALL reflect the new state

#### Scenario: UpdateProvider delegates to UpdateExecutor

- **WHEN** an update operation is invoked on UpdateProvider
- **THEN** it SHALL delegate to UpdateExecutor with the correct resource

#### Scenario: Provider factories create providers for each resource type

- **WHEN** a provider factory is asked to create providers
- **THEN** it SHALL create one provider per configured resource type

### Requirement: Phase 2 unit tests for medium-complexity classes

Unit tests SHALL be added for classes that require HTTP mocking, servlet
mocking, or moderate dependency injection. These classes currently have 5-30%
coverage.

Target files:

- `ImportManifestInterceptor.java` (8.0%, 98 lines)
- `OidcDiscoveryFetcher.java` (5.9%, 30 lines)
- `ExportOperationHelper.java` (21.2%, 29 lines)
- `SparkJobListener.java` (10.0%, 44 lines)
- `ImportLock.java` (22.2%, 7 lines)
- `Spark.java` (3.1%, 30 lines)
- `SubmitterConfiguration.java` (3.7%, 15 lines)
- `ErrorReportingInterceptor.java` (11.1%, 10 lines)

#### Scenario: ImportManifestInterceptor converts JSON to FHIR Parameters

- **WHEN** a request with JSON content type containing a manifest is received
- **THEN** the interceptor SHALL convert it to a FHIR Parameters resource and
  update the content type header

#### Scenario: ImportManifestInterceptor passes through non-JSON requests

- **WHEN** a request with a non-JSON content type is received
- **THEN** the interceptor SHALL pass it through unmodified

#### Scenario: OidcDiscoveryFetcher retrieves OIDC configuration

- **WHEN** the fetcher is asked for OIDC discovery data from a valid issuer URL
- **THEN** it SHALL return the parsed OIDC configuration document

#### Scenario: OidcDiscoveryFetcher handles HTTP errors

- **WHEN** the OIDC discovery endpoint returns a non-2xx response
- **THEN** the fetcher SHALL throw an appropriate exception

#### Scenario: SparkJobListener tracks stage progress

- **WHEN** a Spark stage completion event is received
- **THEN** the listener SHALL update the corresponding job's progress in the
  JobRegistry

### Requirement: Phase 3 unit tests for complex classes

Unit tests SHALL be added for classes with deep framework integration. These
classes currently have 7-25% coverage and require careful mocking of Spring,
HAPI FHIR, and Spark dependencies.

Target files:

- `ViewDefinitionExportProvider.java` (7.1%, 118 lines)
- `ImportPnpExecutor.java` (10.6%, 142 lines)
- `ImportPnpProvider.java` (11.9%, 51 lines)
- `ImportProvider.java` (14.1%, 54 lines)
- `FhirServer.java` (19.4%, 185 lines)
- `PathlingJwtDecoderBuilder.java` (7.0%, 47 lines)
- `PatientExportProvider.java` (12.5%, 26 lines)
- `GroupExportProvider.java` (38.9%, 18 lines)
- `BulkSubmitAuthProvider.java` (24.6%, 51 lines)

#### Scenario: ViewDefinitionExportProvider validates view parameters

- **WHEN** a view definition export operation is invoked with invalid parameters
- **THEN** the provider SHALL return an appropriate error response

#### Scenario: ImportProvider processes a valid import request

- **WHEN** a valid import request with Parameters is received
- **THEN** the provider SHALL create an async job and return a job identifier

#### Scenario: PathlingJwtDecoderBuilder constructs a decoder from OIDC config

- **WHEN** a JWT decoder is built with valid OIDC configuration
- **THEN** the decoder SHALL validate tokens against the configured JWKS endpoint

#### Scenario: FhirServer registers all configured providers

- **WHEN** the FHIR server is initialised
- **THEN** all resource providers and operation providers SHALL be registered

### Requirement: Coverage target

After all phases are complete, the server module overall coverage (as measured
by JaCoCo) SHALL be at least 80%, up from the current 64.4%.

#### Scenario: Coverage meets target after all test additions

- **WHEN** all phase 1, 2, and 3 tests are implemented and passing
- **THEN** the JaCoCo report SHALL show at least 80% overall coverage for the
  server module
