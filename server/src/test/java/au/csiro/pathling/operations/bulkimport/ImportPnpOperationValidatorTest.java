/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.config.ImportConfiguration;
import au.csiro.pathling.config.PnpConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.time.Instant;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Tests for ImportPnpOperationValidator.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
@Import(ImportPnpOperationValidatorTest.TestConfig.class)
class ImportPnpOperationValidatorTest {

  @TestConfiguration
  static class TestConfig {

    @Bean
    public ImportPnpOperationValidator importPnpOperationValidator() {
      final ServerConfiguration serverConfig = new ServerConfiguration();
      final ImportConfiguration importConfig = new ImportConfiguration();
      final PnpConfiguration pnpConfig = new PnpConfiguration();
      importConfig.setPnp(pnpConfig);
      serverConfig.setImport(importConfig);
      return new ImportPnpOperationValidator(serverConfig);
    }
  }

  @Autowired private ImportPnpOperationValidator validator;

  private RequestDetails mockRequest;

  @BeforeEach
  void setUp() {
    mockRequest = MockUtil.mockRequest("application/fhir+json", "respond-async", false);
  }

  // ========================================
  // Valid Request Tests
  // ========================================

  /** Tests that a valid request with all CodeType parameters is accepted. */
  @Test
  void validatesRequestWithAllCodeTypeParameters() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("exportType").setValue(new CodeType("dynamic"));
    params.addParameter().setName("saveMode").setValue(new CodeType("merge"));
    params.addParameter().setName("inputFormat").setValue(new CodeType("application/fhir+ndjson"));

    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ImportPnpRequest> result =
                  validator.validateParametersRequest(mockRequest, params);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().exportUrl()).isEqualTo("https://example.org/fhir/$export");
              assertThat(result.result().exportType()).isEqualTo("dynamic");
              assertThat(result.result().saveMode()).isEqualTo(SaveMode.MERGE);
              assertThat(result.result().importFormat()).isEqualTo(ImportFormat.NDJSON);
            });
  }

  /** Tests that default values are used when optional parameters are omitted. */
  @Test
  void usesDefaultValuesWhenOptionalParametersOmitted() {
    // Only required parameter is exportUrl.
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().exportType()).isEqualTo("dynamic"); // Default.
    assertThat(result.result().saveMode()).isEqualTo(SaveMode.OVERWRITE); // Default.
    assertThat(result.result().importFormat()).isEqualTo(ImportFormat.NDJSON); // Default.
  }

  // ========================================
  // Error Cases - All Must Throw InvalidUserInputError (400)
  // ========================================

  /** Tests that an invalid exportType value throws InvalidUserInputError (results in 400). */
  @Test
  void rejectsInvalidExportType() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("exportType").setValue(new CodeType("invalid-type"));

    assertThatCode(() -> validator.validateParametersRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("Invalid exportType");
  }

  /** Tests that an invalid mode value throws InvalidUserInputError (results in 400). */
  @Test
  void rejectsInvalidMode() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("saveMode").setValue(new CodeType("invalid-mode"));

    assertThatCode(() -> validator.validateParametersRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class);
  }

  /** Tests that an invalid inputFormat value throws InvalidUserInputError (results in 400). */
  @Test
  void rejectsInvalidInputFormat() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("inputFormat").setValue(new CodeType("invalid-format"));

    assertThatCode(() -> validator.validateParametersRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class);
  }

  /** Tests that missing required exportUrl throws InvalidUserInputError (results in 400). */
  @Test
  void rejectsMissingExportUrl() {
    final Parameters params = new Parameters();
    // No exportUrl parameter.

    assertThatCode(() -> validator.validateParametersRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("exportUrl");
  }

  /**
   * Tests that wrong parameter type (Coding when CodeType expected) throws InvalidUserInputError
   * (results in 400), not ClassCastException (which would result in 500).
   */
  @Test
  void rejectsWrongParameterType() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    // Send Coding when CodeType is expected.
    params.addParameter().setName("exportType").setValue(new Coding().setCode("dynamic"));

    // This should throw InvalidUserInputError, NOT ClassCastException.
    assertThatCode(() -> validator.validateParametersRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class);
  }

  /**
   * Tests that inputFormat sent as Coding (wrong type) throws InvalidUserInputError (results in
   * 400).
   */
  @Test
  void rejectsInputFormatAsWrongType() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    // Send Coding when CodeType is expected for inputFormat.
    params
        .addParameter()
        .setName("inputFormat")
        .setValue(new Coding().setCode("application/fhir+ndjson"));

    // This should throw InvalidUserInputError, NOT ClassCastException.
    assertThatCode(() -> validator.validateParametersRequest(mockRequest, params))
        .isInstanceOf(InvalidUserInputError.class);
  }

  // ========================================
  // Valid Export Type Values
  // ========================================

  @Test
  void acceptsStaticExportType() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("exportType").setValue(new CodeType("static"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().exportType()).isEqualTo("static");
  }

  // ========================================
  // Valid Mode Values
  // ========================================

  @Test
  void acceptsOverwriteMode() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("saveMode").setValue(new CodeType("overwrite"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().saveMode()).isEqualTo(SaveMode.OVERWRITE);
  }

  @Test
  void acceptsMergeMode() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("saveMode").setValue(new CodeType("merge"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().saveMode()).isEqualTo(SaveMode.MERGE);
  }

  // ========================================
  // Valid Input Format Values
  // ========================================

  @Test
  void acceptsNdjsonFormat() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("inputFormat").setValue(new CodeType("application/fhir+ndjson"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().importFormat()).isEqualTo(ImportFormat.NDJSON);
  }

  @Test
  void acceptsParquetFormat() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params
        .addParameter()
        .setName("inputFormat")
        .setValue(new CodeType("application/x-pathling-parquet"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().importFormat()).isEqualTo(ImportFormat.PARQUET);
  }

  // ========================================
  // Bulk Data Export Passthrough Parameters
  // ========================================

  /** Tests that _type parameter with a single value is extracted correctly. */
  @Test
  void extractsSingleTypeParameter() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("_type").setValue(new StringType("Patient"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().types()).containsExactly("Patient");
  }

  /** Tests that _type parameter with multiple values is extracted correctly. */
  @Test
  void extractsMultipleTypeParameters() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("_type").setValue(new StringType("Patient"));
    params.addParameter().setName("_type").setValue(new StringType("Observation"));
    params.addParameter().setName("_type").setValue(new StringType("Condition"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().types()).containsExactly("Patient", "Observation", "Condition");
  }

  /** Tests that _type defaults to an empty list when not provided. */
  @Test
  void defaultsTypeToEmptyList() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().types()).isEmpty();
  }

  /** Tests that _since parameter is extracted correctly. */
  @Test
  void extractsSinceParameter() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    final Instant sinceTime = Instant.parse("2024-01-15T10:30:00Z");
    params
        .addParameter()
        .setName("_since")
        .setValue(new InstantType(java.util.Date.from(sinceTime)));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().since()).isPresent();
    assertThat(result.result().since().get()).isEqualTo(sinceTime);
  }

  /** Tests that _since defaults to empty when not provided. */
  @Test
  void defaultsSinceToEmpty() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().since()).isEmpty();
  }

  /** Tests that _until parameter is extracted correctly. */
  @Test
  void extractsUntilParameter() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    final Instant untilTime = Instant.parse("2024-06-30T23:59:59Z");
    params
        .addParameter()
        .setName("_until")
        .setValue(new InstantType(java.util.Date.from(untilTime)));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().until()).isPresent();
    assertThat(result.result().until().get()).isEqualTo(untilTime);
  }

  /** Tests that _until defaults to empty when not provided. */
  @Test
  void defaultsUntilToEmpty() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().until()).isEmpty();
  }

  /** Tests that _elements parameter with multiple values is extracted correctly. */
  @Test
  void extractsElementsParameters() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("_elements").setValue(new StringType("id"));
    params.addParameter().setName("_elements").setValue(new StringType("name"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().elements()).containsExactly("id", "name");
  }

  /** Tests that _elements defaults to an empty list when not provided. */
  @Test
  void defaultsElementsToEmptyList() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().elements()).isEmpty();
  }

  /** Tests that _typeFilter parameter with multiple values is extracted correctly. */
  @Test
  void extractsTypeFilterParameters() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("_typeFilter").setValue(new StringType("Patient?active=true"));
    params
        .addParameter()
        .setName("_typeFilter")
        .setValue(new StringType("Observation?status=final"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().typeFilters())
        .containsExactly("Patient?active=true", "Observation?status=final");
  }

  /** Tests that _typeFilter defaults to an empty list when not provided. */
  @Test
  void defaultsTypeFiltersToEmptyList() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().typeFilters()).isEmpty();
  }

  /** Tests that includeAssociatedData parameter with multiple values is extracted correctly. */
  @Test
  void extractsIncludeAssociatedDataParameters() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params
        .addParameter()
        .setName("includeAssociatedData")
        .setValue(new CodeType("LatestProvenanceResources"));
    params
        .addParameter()
        .setName("includeAssociatedData")
        .setValue(new CodeType("RelevantProvenanceResources"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().includeAssociatedData())
        .containsExactly("LatestProvenanceResources", "RelevantProvenanceResources");
  }

  /** Tests that includeAssociatedData defaults to an empty list when not provided. */
  @Test
  void defaultsIncludeAssociatedDataToEmptyList() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().includeAssociatedData()).isEmpty();
  }

  // ========================================
  // Unauthenticated Configuration
  // ========================================

  /** Tests that validation passes when no authentication is configured (unauthenticated source). */
  @Test
  void acceptsUnauthenticatedConfiguration() {
    // Create a validator with no authentication credentials configured.
    final ServerConfiguration serverConfig = new ServerConfiguration();
    final ImportConfiguration importConfig = new ImportConfiguration();
    final PnpConfiguration pnpConfig = new PnpConfiguration();
    // No clientId, clientSecret, or privateKeyJwk set - unauthenticated.
    importConfig.setPnp(pnpConfig);
    serverConfig.setImport(importConfig);
    final ImportPnpOperationValidator unauthenticatedValidator =
        new ImportPnpOperationValidator(serverConfig);

    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://public-fhir-server.org/$export"));

    // Should succeed without requiring authentication credentials.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ImportPnpRequest> result =
                  unauthenticatedValidator.validateParametersRequest(mockRequest, params);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().exportUrl())
                  .isEqualTo("https://public-fhir-server.org/$export");
            });
  }

  /** Tests that all export parameters are extracted together correctly. */
  @Test
  void extractsAllExportParametersTogether() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params.addParameter().setName("_type").setValue(new StringType("Patient"));
    params.addParameter().setName("_type").setValue(new StringType("Observation"));
    final Instant sinceTime = Instant.parse("2024-01-01T00:00:00Z");
    params
        .addParameter()
        .setName("_since")
        .setValue(new InstantType(java.util.Date.from(sinceTime)));
    final Instant untilTime = Instant.parse("2024-12-31T23:59:59Z");
    params
        .addParameter()
        .setName("_until")
        .setValue(new InstantType(java.util.Date.from(untilTime)));
    params.addParameter().setName("_elements").setValue(new StringType("id"));
    params.addParameter().setName("_typeFilter").setValue(new StringType("Patient?active=true"));
    params
        .addParameter()
        .setName("includeAssociatedData")
        .setValue(new CodeType("LatestProvenanceResources"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    final ImportPnpRequest request = result.result();
    assertThat(request.types()).containsExactly("Patient", "Observation");
    assertThat(request.since()).contains(sinceTime);
    assertThat(request.until()).contains(untilTime);
    assertThat(request.elements()).containsExactly("id");
    assertThat(request.typeFilters()).containsExactly("Patient?active=true");
    assertThat(request.includeAssociatedData()).containsExactly("LatestProvenanceResources");
  }
}
