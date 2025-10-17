package au.csiro.pathling.import_;

import static org.assertj.core.api.Assertions.LIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.operations.import_.ImportFormat;
import au.csiro.pathling.operations.import_.ImportOperationValidator;
import au.csiro.pathling.operations.import_.ImportRequest;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.MockUtil;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
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
  // Header Validation Tests
  // ========================================

  @ParameterizedTest
  @MethodSource("provide_headers")
  void test_headers(String acceptHeader, String preferHeader, boolean lenient,
      List<String> messages, boolean valid) {
    Parameters minimalParams = minimalValidParams();

    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        PreAsyncValidationResult<ImportRequest> result = importOperationValidator.validateRequest(
            MockUtil.mockRequest(acceptHeader, preferHeader, lenient), minimalParams);
        assertThat(result)
            .isNotNull()
            .extracting(PreAsyncValidationResult::warnings, LIST)
            .map(OperationOutcome.OperationOutcomeIssueComponent.class::cast)
            .map(issue -> issue.getDetails().getText())
            .containsExactlyInAnyOrderElementsOf(messages);
      });
    } else {
      assertThatCode(() -> importOperationValidator.validateRequest(
              MockUtil.mockRequest(acceptHeader, preferHeader, lenient), minimalParams))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provide_headers() {
    return Stream.of(
        // strict mode - valid
        arguments("application/fhir+json", "respond-async", false, List.of(), true),
        // strict mode - invalid Accept header
        arguments("application/fhir+ndjson", "respond-async", false, List.of(), false),
        arguments("ndjson", "respond-async", false, List.of(), false),
        arguments(null, "respond-async", false, List.of(), false),
        // strict mode - invalid Prefer header
        arguments("application/fhir+json", null, false, List.of(), false),
        arguments("application/fhir+json", "abc", false, List.of(), false),
        // strict mode - both headers missing/invalid
        arguments(null, null, false, List.of(), false),

        // lenient mode - valid
        arguments("application/fhir+json", "respond-async", true, List.of(), true),
        // lenient mode - fixes Accept header
        arguments("application/fhir+ndjson", "respond-async", true,
            List.of("Added missing header: Accept application/fhir+json"), true),
        arguments("ndjson", "respond-async", true,
            List.of("Added missing header: Accept application/fhir+json"), true),
        arguments(null, "respond-async", true,
            List.of("Added missing header: Accept application/fhir+json"), true),
        // lenient mode - fixes Prefer header
        arguments("application/fhir+json", null, true,
            List.of("Added missing header: Prefer respond-async"), true),
        arguments("application/fhir+json", "abc", true,
            List.of("Added missing header: Prefer respond-async"), true),
        // lenient mode - fixes both headers
        arguments(null, null, true,
            List.of("Added missing header: Accept application/fhir+json",
                "Added missing header: Prefer respond-async"), true)
    );
  }

  // ========================================
  // Input Parameter Tests
  // ========================================

  @ParameterizedTest
  @MethodSource("provide_input_params")
  void test_input_params(Parameters params, boolean lenient, Map<ResourceType, String> expectedInput,
      boolean valid) {
    RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json", "respond-async", lenient);

    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        PreAsyncValidationResult<ImportRequest> result = importOperationValidator.validateRequest(
            mockRequest, params);
        assertThat(result.result().input()).isEqualTo(expectedInput);
      });
    } else {
      assertThatCode(() -> importOperationValidator.validateRequest(mockRequest, params))
          .isInstanceOfAny(InvalidUserInputError.class, UnprocessableEntityException.class);
    }
  }

  private static Stream<Arguments> provide_input_params() {
    return Stream.of(
        // Valid single input
        arguments(
            paramsWithInputs(input("Patient", "s3://bucket/Patient.ndjson")),
            false,
            Map.of(ResourceType.PATIENT, "s3://bucket/Patient.ndjson"),
            true
        ),
        // Valid multiple inputs
        arguments(
            paramsWithInputs(
                input("Patient", "s3://bucket/Patient.ndjson"),
                input("Observation", "s3://bucket/Observation.ndjson")
            ),
            false,
            Map.of(
                ResourceType.PATIENT, "s3://bucket/Patient.ndjson",
                ResourceType.OBSERVATION, "s3://bucket/Observation.ndjson"
            ),
            true
        ),
        // Missing input parameter - strict mode
        arguments(new Parameters(), false, null, false),
        // Missing input parameter - lenient mode (should skip)
        arguments(new Parameters(), true, Map.of(), true),
        // Missing resourceType - strict mode
        arguments(paramsWithInputs(inputWithoutResourceType("s3://bucket/data.ndjson")), false, null, false),
        // Missing resourceType - lenient mode (should skip)
        arguments(paramsWithInputs(inputWithoutResourceType("s3://bucket/data.ndjson")), true, Map.of(), true),
        // Missing url - strict mode
        arguments(paramsWithInputs(inputWithoutUrl("Patient")), false, null, false),
        // Missing url - lenient mode (should skip)
        arguments(paramsWithInputs(inputWithoutUrl("Patient")), true, Map.of(), true),
        // Invalid resource type - strict mode
        arguments(paramsWithInputs(input("InvalidType", "s3://bucket/data.ndjson")), false, null, false),
        // Unsupported resource type - strict mode
        arguments(paramsWithInputs(input("Bundle", "s3://bucket/data.ndjson")), false, null, false),
        // Unsupported resource type - lenient mode
        arguments(paramsWithInputs(input("Bundle", "s3://bucket/data.ndjson")), true, Map.of(), true),
        // Unsupported resource type and supported resource type - lenient mode
        arguments(paramsWithInputs(
            input("Bundle", "s3://bucket/data.ndjson"), 
            input("Patient", "s3://bucket/data.ndjson")
            ), true, Map.of(ResourceType.PATIENT, "s3://bucket/data.ndjson"), true),
        // Mix of valid and invalid - lenient mode (should keep valid only)
        arguments(
            paramsWithInputs(
                input("Patient", "s3://bucket/Patient.ndjson"),
                inputWithoutUrl("Observation")
            ),
            true,
            Map.of(ResourceType.PATIENT, "s3://bucket/Patient.ndjson"),
            true
        )
    );
  }

  // ========================================
  // Mode Parameter Tests
  // ========================================

  @ParameterizedTest
  @MethodSource("provide_mode_params")
  void test_mode_param(Parameters params, boolean lenient, SaveMode expectedMode, boolean valid) {
    RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json", "respond-async", lenient);

    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        PreAsyncValidationResult<ImportRequest> result = importOperationValidator.validateRequest(
            mockRequest, params);
        assertThat(result.result().saveMode()).isEqualTo(expectedMode);
      });
    } else {
      assertThatCode(() -> importOperationValidator.validateRequest(mockRequest, params))
          .isInstanceOf(InvalidUserInputError.class);
    }
  }

  private static Stream<Arguments> provide_mode_params() {
    return Stream.of(
        // No mode specified - defaults to OVERWRITE
        arguments(minimalValidParams(), false, SaveMode.OVERWRITE, true),
        // Explicit OVERWRITE mode
        arguments(paramsWithMode("overwrite"), false, SaveMode.OVERWRITE, true),
        // Explicit MERGE mode
        arguments(paramsWithMode("merge"), false, SaveMode.MERGE, true),
        // Invalid mode code - strict
        arguments(paramsWithMode("invalid"), false, null, false),
        // Invalid mode code - lenient (defaults to OVERWRITE)
        arguments(paramsWithMode("invalid"), true, SaveMode.OVERWRITE, true)
    );
  }

  // ========================================
  // Format Parameter Tests
  // ========================================

  @ParameterizedTest
  @MethodSource("provide_format_params")
  void test_format_param(Parameters params, boolean lenient, ImportFormat expectedFormat, boolean valid) {
    RequestDetails mockRequest = MockUtil.mockRequest("application/fhir+json", "respond-async", lenient);

    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        PreAsyncValidationResult<ImportRequest> result = importOperationValidator.validateRequest(
            mockRequest, params);
        assertThat(result.result().importFormat()).isEqualTo(expectedFormat);
      });
    } else {
      assertThatCode(() -> importOperationValidator.validateRequest(mockRequest, params))
          .isInstanceOf(InvalidUserInputError.class);
    }
  }

  private static Stream<Arguments> provide_format_params() {
    return Stream.of(
        // No format specified - defaults to NDJSON
        arguments(minimalValidParams(), false, ImportFormat.NDJSON, true),
        // Explicit NDJSON format
        arguments(paramsWithFormat("ndjson"), false, ImportFormat.NDJSON, true),
        // Explicit PARQUET format
        arguments(paramsWithFormat("parquet"), false, ImportFormat.PARQUET, true),
        // Explicit DELTA format
        arguments(paramsWithFormat("delta"), false, ImportFormat.DELTA, true),
        // Invalid format - strict
        arguments(paramsWithFormat("invalid"), false, null, false),
        // Invalid format - lenient (defaults to NDJSON)
        arguments(paramsWithFormat("invalid"), true, ImportFormat.NDJSON, true)
    );
  }

  // ========================================
  // Lenient Mode Tests
  // ========================================

  @ParameterizedTest
  @MethodSource("provide_lenient_scenarios")
  void test_lenient_mode(Parameters params, String acceptHeader, String preferHeader,
      boolean lenient, boolean expectSuccess) {
    RequestDetails mockRequest = MockUtil.mockRequest(acceptHeader, preferHeader, lenient);

    if (expectSuccess) {
      assertThatNoException().isThrownBy(() -> {
        PreAsyncValidationResult<ImportRequest> result = importOperationValidator.validateRequest(
            mockRequest, params);
        assertThat(result.result().lenient()).isEqualTo(lenient);
      });
    } else {
      assertThatCode(() -> importOperationValidator.validateRequest(mockRequest, params))
          .isExactlyInstanceOf(InvalidUserInputError.class);
    }
  }

  private static Stream<Arguments> provide_lenient_scenarios() {
    return Stream.of(
        // Lenient mode allows partial failures
        arguments(
            paramsWithInputs(
                input("Patient", "s3://bucket/Patient.ndjson"),
                inputWithoutUrl("Observation")
            ),
            "application/fhir+json",
            "respond-async",
            true,
            true
        ),
        // Strict mode fails on partial failures
        arguments(
            paramsWithInputs(
                input("Patient", "s3://bucket/Patient.ndjson"),
                inputWithoutUrl("Observation")
            ),
            "application/fhir+json",
            "respond-async",
            false,
            false
        )
    );
  }

  // ========================================
  // Helper Methods
  // ========================================

  private static Parameters minimalValidParams() {
    return paramsWithInputs(input("Patient", "s3://bucket/Patient.ndjson"));
  }

  private static Parameters paramsWithInputs(ParametersParameterComponent... inputs) {
    Parameters parameters = new Parameters();
    for (ParametersParameterComponent input : inputs) {
      parameters.addParameter(input);
    }
    return parameters;
  }

  private static Parameters paramsWithMode(String modeCode) {
    Parameters params = minimalValidParams();
    params.addParameter()
        .setName("mode")
        .setValue(new Coding().setCode(modeCode));
    return params;
  }

  private static Parameters paramsWithFormat(String formatCode) {
    Parameters params = minimalValidParams();
    params.addParameter()
        .setName("format")
        .setValue(new Coding().setCode(formatCode));
    return params;
  }

  private static ParametersParameterComponent input(String resourceType, String url) {
    ParametersParameterComponent input = new ParametersParameterComponent();
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
    ParametersParameterComponent input = new ParametersParameterComponent();
    input.setName("input");
    input.addPart()
        .setName("url")
        .setValue(new UrlType(url));
    return input;
  }

  private static ParametersParameterComponent inputWithoutUrl(String resourceType) {
    ParametersParameterComponent input = new ParametersParameterComponent();
    input.setName("input");
    input.addPart()
        .setName("resourceType")
        .setValue(new Coding().setCode(resourceType));
    return input;
  }
}
