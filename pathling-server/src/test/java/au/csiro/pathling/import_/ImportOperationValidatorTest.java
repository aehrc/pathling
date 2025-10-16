package au.csiro.pathling.import_;

import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.operations.import_.ImportOperationValidator;
import au.csiro.pathling.operations.import_.ImportRequest;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * @author Felix Naumann
 */
@SpringBootUnitTest
class ImportOperationValidatorTest {
  
  @Autowired
  private ImportOperationValidator importOperationValidator;
  
  @ParameterizedTest
  @MethodSource("provide_import_requests")
  void test_import_validator(Parameters requestParams, String acceptHeader, String preferHeader, boolean lenient, ImportRequest expectedResult) {
    assertThatNoException().isThrownBy(() -> {
      PreAsyncValidationResult<ImportRequest> result = importOperationValidator.validateRequest(MockUtil.mockRequest(acceptHeader, preferHeader, lenient), requestParams);
      assertThat(result.result()).isEqualTo(expectedResult);
    });
  }
  
  private static Stream<Arguments> provide_import_requests() {
    return Stream.of(
        arguments(minimal_valid_params(), minimal_valid_headers())
    );
  }
  
  private static Map<String, String> minimal_valid_headers() {
    return Map.of(
        "Accept", "application/fhir+json",
        "Prefer", "respond-async"
    );
  }
  
  private static Parameters minimal_valid_params() {
    Parameters parameters = new Parameters();
    parameters.addParameter(input("Patient", "Patient.ndjson"));
    return parameters;
  }
  
  private static Parameters.ParametersParameterComponent input(String resourceType, String url) {
    ParametersParameterComponent input = new ParametersParameterComponent();
    input.setName("input");
    input.addPart()
        .setName("resourceType")
        .setValue(new CodeType(resourceType));
    input.addPart()
        .setName("url")
        .setValue(new UrlType(url));
    return input;
  }
}
