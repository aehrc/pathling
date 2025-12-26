package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.bulkimport.ImportFormat;
import au.csiro.pathling.operations.bulkimport.ImportManifest;
import au.csiro.pathling.operations.bulkimport.ImportManifestInput;
import au.csiro.pathling.operations.bulkimport.ImportOperationValidator;
import au.csiro.pathling.operations.bulkimport.ImportRequest;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Tests for ImportOperationValidator.
 *
 * @author Felix Naumann
 */
@SpringBootUnitTest
@Import(ImportOperationValidatorTest.TestConfig.class)
class ImportOperationValidatorTest {

  @TestConfiguration
  static class TestConfig {

    @Bean
    public ImportOperationValidator importOperationValidator() {
      return new ImportOperationValidator();
    }
  }

  @Autowired
  private ImportOperationValidator importOperationValidator;

  // ========================================
  // Parameters Format Tests
  // ========================================

  @Test
  void test_parametersRequest_valid() {
    final Parameters params = minimalValidParams();
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatNoException().isThrownBy(() -> {
      final PreAsyncValidationResult<ImportRequest> result =
          importOperationValidator.validateParametersRequest(mockRequest, params);
      assertThat(result.result()).isNotNull();
      assertThat(result.result().input()).containsKey("Patient");
    });
  }

  @Test
  void parametersRequestWithoutInputSourceSucceeds() {
    // inputSource is optional - should not throw when missing.
    final Parameters params = new Parameters();
    params.addParameter(input("Patient", "s3://bucket/Patient.ndjson"));
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatNoException().isThrownBy(
        () -> importOperationValidator.validateParametersRequest(mockRequest, params));
  }

  @ParameterizedTest
  @MethodSource("provide_input_params")
  void test_input_params(Parameters params, Map<String, Collection<String>> expectedInput,
      boolean valid) {
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        final PreAsyncValidationResult<ImportRequest> result =
            importOperationValidator.validateParametersRequest(mockRequest, params);
        assertThat(result.result()).isNotNull();
        assertThat(result.result().input()).isEqualTo(expectedInput);
      });
    } else {
      assertThatCode(() -> importOperationValidator.validateParametersRequest(mockRequest, params))
          .isInstanceOf(InvalidUserInputError.class);
    }
  }

  private static Stream<Arguments> provide_input_params() {
    return Stream.of(
        // Valid single input
        arguments(
            paramsWithInputs(input("Patient", "s3://bucket/Patient.ndjson")),
            Map.of("Patient", List.of("s3a://bucket/Patient.ndjson")),
            true
        ),
        // Valid multiple inputs
        arguments(
            paramsWithInputs(
                input("Patient", "s3://bucket/Patient.ndjson"),
                input("Observation", "s3://bucket/Observation.ndjson")
            ),
            Map.of(
                "Patient", List.of("s3a://bucket/Patient.ndjson"),
                "Observation", List.of("s3a://bucket/Observation.ndjson")
            ),
            true
        ),
        // Missing input parameter
        arguments(paramsWithOnlyInputSource(), null, false),
        // Missing resourceType
        arguments(paramsWithInputs(inputWithoutResourceType("s3://bucket/data.ndjson")), null,
            false),
        // Missing url
        arguments(paramsWithInputs(inputWithoutUrl("Patient")), null, false)
    );
  }

  @ParameterizedTest
  @MethodSource("provide_mode_params")
  void test_mode_param(Parameters params, SaveMode expectedMode) {
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    final PreAsyncValidationResult<ImportRequest> result =
        importOperationValidator.validateParametersRequest(mockRequest, params);
    assertThat(result.result().saveMode()).isEqualTo(expectedMode);
  }

  private static Stream<Arguments> provide_mode_params() {
    return Stream.of(
        arguments(minimalValidParams(), SaveMode.OVERWRITE),
        arguments(paramsWithMode("overwrite"), SaveMode.OVERWRITE),
        arguments(paramsWithMode("merge"), SaveMode.MERGE),
        arguments(paramsWithMode("error"), SaveMode.ERROR_IF_EXISTS),
        arguments(paramsWithMode("append"), SaveMode.APPEND),
        arguments(paramsWithMode("ignore"), SaveMode.IGNORE)
    );
  }

  @ParameterizedTest
  @MethodSource("provide_format_params")
  void test_format_param(Parameters params, ImportFormat expectedFormat) {
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    final PreAsyncValidationResult<ImportRequest> result =
        importOperationValidator.validateParametersRequest(mockRequest, params);
    assertThat(result.result().importFormat()).isEqualTo(expectedFormat);
  }

  private static Stream<Arguments> provide_format_params() {
    return Stream.of(
        arguments(minimalValidParams(), ImportFormat.NDJSON),
        arguments(paramsWithFormat("application/fhir+ndjson"), ImportFormat.NDJSON),
        arguments(paramsWithFormat("application/x-pathling-parquet"), ImportFormat.PARQUET),
        arguments(paramsWithFormat("application/x-pathling-delta+parquet"), ImportFormat.DELTA)
    );
  }

  // ========================================
  // JSON Format Tests
  // ========================================

  @Test
  void test_jsonRequest_valid() {
    final ImportManifest manifest = new ImportManifest(
        "application/fhir+ndjson",
        "https://example.org/source",
        List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
        null
    );
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    final PreAsyncValidationResult<ImportRequest> result =
        importOperationValidator.validateJsonRequest(mockRequest, manifest);

    assertThat(result.result()).isNotNull();
    assertThat(result.result().importFormat()).isEqualTo(ImportFormat.NDJSON);
  }

  @Test
  void jsonRequestWithoutInputSourceSucceeds() {
    // inputSource is optional - should not throw when missing.
    final ImportManifest manifest = new ImportManifest(
        "application/fhir+ndjson",
        null,  // Optional
        List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
        null
    );
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    assertThatNoException().isThrownBy(
        () -> importOperationValidator.validateJsonRequest(mockRequest, manifest));
  }

  @Test
  void test_jsonRequest_withModeParameter() {
    final ImportManifest manifest = new ImportManifest(
        "application/fhir+ndjson",
        "https://example.org/source",
        List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
        "merge"
    );
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    final PreAsyncValidationResult<ImportRequest> result =
        importOperationValidator.validateJsonRequest(mockRequest, manifest);

    assertThat(result.result().saveMode()).isEqualTo(SaveMode.MERGE);
  }

  @ParameterizedTest
  @MethodSource("provide_mimeTypes")
  void test_mimeTypeFormatParsing(String mimeType, ImportFormat expectedFormat) {
    final ImportManifest manifest = new ImportManifest(
        mimeType,
        "https://example.org/source",
        List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
        null
    );
    final RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json",
        "respond-async", false);

    final PreAsyncValidationResult<ImportRequest> result =
        importOperationValidator.validateJsonRequest(mockRequest, manifest);

    assertThat(result.result().importFormat()).isEqualTo(expectedFormat);
  }

  private static Stream<Arguments> provide_mimeTypes() {
    return Stream.of(
        arguments("application/fhir+ndjson", ImportFormat.NDJSON),
        arguments("application/x-pathling-parquet", ImportFormat.PARQUET),
        arguments("application/x-pathling-delta+parquet", ImportFormat.DELTA)
    );
  }

  // ========================================
  // Helper Methods
  // ========================================

  private static Parameters minimalValidParams() {
    return paramsWithInputs(input("Patient", "s3://bucket/Patient.ndjson"));
  }

  private static Parameters paramsWithOnlyInputSource() {
    final Parameters parameters = new Parameters();
    parameters.addParameter()
        .setName("inputSource")
        .setValue(new StringType("https://example.org/source"));
    return parameters;
  }

  private static Parameters paramsWithInputs(ParametersParameterComponent... inputs) {
    final Parameters parameters = new Parameters();
    parameters.addParameter()
        .setName("inputSource")
        .setValue(new StringType("https://example.org/source"));
    for (final ParametersParameterComponent input : inputs) {
      parameters.addParameter(input);
    }
    return parameters;
  }

  private static Parameters paramsWithMode(String modeCode) {
    final Parameters params = minimalValidParams();
    params.addParameter()
        .setName("saveMode")
        .setValue(new Coding().setCode(modeCode));
    return params;
  }

  private static Parameters paramsWithFormat(String formatCode) {
    final Parameters params = minimalValidParams();
    params.addParameter()
        .setName("inputFormat")  // Changed from "format"
        .setValue(new Coding().setCode(formatCode));
    return params;
  }

  private static ParametersParameterComponent input(String resourceType, String url) {
    final ParametersParameterComponent input = new ParametersParameterComponent();
    input.setName("input");
    input.addPart()
        .setName("resourceType")
        .setValue(new Coding().setCode(resourceType));
    input.addPart()
        .setName("url")
        .setValue(new UrlType(url));
    return input;
  }

  private static ParametersParameterComponent inputWithoutResourceType(String url) {
    final ParametersParameterComponent input = new ParametersParameterComponent();
    input.setName("input");
    input.addPart()
        .setName("url")
        .setValue(new UrlType(url));
    return input;
  }

  private static ParametersParameterComponent inputWithoutUrl(String resourceType) {
    final ParametersParameterComponent input = new ParametersParameterComponent();
    input.setName("input");
    input.addPart()
        .setName("resourceType")
        .setValue(new Coding().setCode(resourceType));
    return input;
  }
}
