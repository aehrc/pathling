/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
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
      // Configure with a client secret so that authentication validation passes.
      pnpConfig.setClientSecret("test-secret");
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

  @Test
  void acceptsDeltaFormat() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("exportUrl")
        .setValue(new UrlType("https://example.org/fhir/$export"));
    params
        .addParameter()
        .setName("inputFormat")
        .setValue(new CodeType("application/x-pathling-delta+parquet"));

    final PreAsyncValidationResult<ImportPnpRequest> result =
        validator.validateParametersRequest(mockRequest, params);

    assertThat(result.result().importFormat()).isEqualTo(ImportFormat.DELTA);
  }
}
