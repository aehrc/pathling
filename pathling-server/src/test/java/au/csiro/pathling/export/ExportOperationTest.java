package au.csiro.pathling.export;

import static au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import static au.csiro.pathling.operations.export.ExportOutputFormat.ND_JSON;
import static au.csiro.pathling.util.ExportOperationUtil.fi;
import static au.csiro.pathling.util.ExportOperationUtil.json;
import static au.csiro.pathling.util.ExportOperationUtil.req;
import static au.csiro.pathling.util.ExportOperationUtil.res;
import static au.csiro.pathling.util.ExportOperationUtil.resolveTempDirIn;
import static au.csiro.pathling.util.ExportOperationUtil.write_details;
import static au.csiro.pathling.util.TestConstants.RESOLVE_PATIENT;
import static au.csiro.pathling.util.TestConstants.WAREHOUSE_PLACEHOLDER;
import static org.assertj.core.api.Assertions.LIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.operations.export.ExportExecutor;
import au.csiro.pathling.operations.export.ExportOperationValidator;
import au.csiro.pathling.operations.export.ExportOutputFormat;
import au.csiro.pathling.operations.export.ExportRequest;
import au.csiro.pathling.operations.export.ExportResponse;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.ExportRequestBuilder;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import scala.collection.JavaConverters;

/**
 * @author Felix Naumann
 */

@Import({
    ExportOperationValidator.class,
    ExportExecutor.class,
    FhirServerTestConfiguration.class
})
@SpringBootUnitTest
class ExportOperationTest {

  @Autowired
  private ExportOperationValidator exportOperationValidator;

  private ObjectMapper objectMapper;

  @MockBean
  private QueryableDataSource queryableDataSource; // NO-SONAR the mock is needed

  @BeforeEach
  void init() {
    SharedMocks.resetAll();
    objectMapper = new ObjectMapper();
  }

  @ParameterizedTest
  @MethodSource("provide_headers")
  void test_headers(String acceptHeader, String preferHeader, boolean lenient,
      List<String> messages, boolean valid) {
    String outputFormat = ExportOutputFormat.asParam(ND_JSON);
    InstantType now = InstantType.now();
    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        PreAsyncValidationResult<ExportRequest> result = exportOperationValidator.validateRequest(
            mockRequest(acceptHeader, preferHeader, lenient), outputFormat, now, null, null, null);
        assertThat(result)
            .isNotNull()
            .extracting(PreAsyncValidationResult::warnings, LIST)
            .map(OperationOutcome.OperationOutcomeIssueComponent.class::cast)
            .map(issue -> issue.getDetails().getText())
            .containsExactlyInAnyOrderElementsOf(messages);
      });
    } else {
      assertThatException().isThrownBy(() -> exportOperationValidator.validateRequest(
              mockRequest(acceptHeader, preferHeader, lenient), outputFormat, now, null, null, null))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provide_headers() {
    return Stream.of(
        // strict
        arguments("application/fhir+json", "respond-async", false, List.of(), true),
        arguments("application/fhir+ndjson", "respond-async", false, List.of(), false),
        arguments("ndjson", "respond-async", false, List.of(), false),
        arguments(null, "respond-async", false, List.of(), false),
        arguments(null, null, false, List.of(), false),
        arguments("application/fhir+json", null, false, List.of(), false),
        arguments("application/fhir+json", "abc", false, List.of(), false),
        // lenient
        arguments("application/fhir+json", "respond-async", true, List.of(), true),
        arguments("application/fhir+ndjson", "respond-async", true,
            List.of("Added missing header: Accept application/fhir+json"), true),
        arguments("ndjson", "respond-async", true,
            List.of("Added missing header: Accept application/fhir+json"), true),
        arguments(null, "respond-async", true,
            List.of("Added missing header: Accept application/fhir+json"), true),
        arguments(null, null, true, List.of("Added missing header: Accept application/fhir+json",
            "Added missing header: Prefer respond-async"), true),
        arguments("application/fhir+json", null, true,
            List.of("Added missing header: Prefer respond-async"), true),
        arguments("application/fhir+json", "abc", true,
            List.of("Added missing header: Prefer respond-async"), true)

    );
  }

  @ParameterizedTest
  @MethodSource("provide_until_param")
  void test_until_param_is_mapped(InstantType until, InstantType expectedUntil) {
    RequestDetails mockReqDetails = mockRequest("application/fhir+json", "respond-async", false);
    ExportRequest actualExportRequest = exportOperationValidator.validateRequest(mockReqDetails,
        ExportOutputFormat.asParam(ND_JSON), InstantType.now(), until, null, null).result();
    assertThat(actualExportRequest).isNotNull();
    assertThat(actualExportRequest.until()).isEqualTo(expectedUntil);
  }

  private static Stream<Arguments> provide_until_param() {
    InstantType now = InstantType.now();
    return Stream.of(
        arguments(now, now),
        arguments(null, null)
    );
  }

  @ParameterizedTest
  @MethodSource("provide_type_param")
  void test_type_param_is_mapped(List<String> types, boolean lenient,
      List<ResourceType> expectedTypes) {
    RequestDetails mockReqDetails = mockRequest("application/fhir+json", "respond-async", lenient);
    InstantType now = InstantType.now();
    if (expectedTypes != null) {
      ExportRequest actualExportRequest = exportOperationValidator.validateRequest(mockReqDetails,
          ExportOutputFormat.asParam(ND_JSON), now, null, types, null).result();
      assertThat(actualExportRequest).isNotNull();
      assertThat(
          actualExportRequest.includeResourceTypeFilters()).containsExactlyInAnyOrderElementsOf(
          expectedTypes);
    } else {
      String outputFormat = ExportOutputFormat.asParam(ND_JSON);
      assertThatCode(
          () -> exportOperationValidator.validateRequest(mockReqDetails, outputFormat, now, null,
              types, null))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provide_type_param() {
    String unsupportedResource = JavaConverters.setAsJavaSet(EncoderBuilder.UNSUPPORTED_RESOURCES())
        .stream().findFirst().orElseThrow();
    return Stream.of(
        arguments(List.of("Patient"), false, List.of(ResourceType.PATIENT)),
        arguments(List.of("Patient", "Observation"), false,
            List.of(ResourceType.PATIENT, ResourceType.OBSERVATION)),
        arguments(List.of("Patient2", "Observation"), false, null),
        // invalid resource type and lenient=false -> error
        arguments(List.of("Patient2", "Observation"), true, List.of(ResourceType.OBSERVATION)),
        // invalid resource type and lenient=true -> pass but ignore invalid resource
        arguments(List.of(unsupportedResource, "Patient"), true, List.of(ResourceType.PATIENT)),
        arguments(List.of(unsupportedResource, "Patient"), false, null),
        arguments(List.of("NULL", "Patient"), false, null),
        arguments(List.of("NULL", "Patient"), true, List.of(ResourceType.PATIENT))
    );
  }

  @ParameterizedTest
  @MethodSource("provide_elements_param")
  void test_elements_param_is_mapped(List<String> elements,
      List<ExportRequest.FhirElement> expectedElements) {
    RequestDetails mockReqDetails = mockRequest("application/fhir+json", "respond-async", false);
    String outputFormat = ExportOutputFormat.asParam(ND_JSON);
    InstantType now = InstantType.now();
    if (expectedElements != null) {
      ExportRequest actualExportRequest = exportOperationValidator.validateRequest(mockReqDetails,
          outputFormat, now, null, null, elements).result();
      assertThat(actualExportRequest).isNotNull();
      assertThat(actualExportRequest.elements()).containsExactlyInAnyOrderElementsOf(
          expectedElements);
    } else {
      assertThatCode(
          () -> exportOperationValidator.validateRequest(mockReqDetails, outputFormat, now, null,
              null, elements))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provide_elements_param() {
    return Stream.of(
        arguments(List.of("Patient.identifier"), List.of(fe("Patient", "identifier"))),
        arguments(List.of("identifier"), List.of(fe(null, "identifier"))),
        arguments(List.of("Patient.identifier", "name"),
            List.of(fe("Patient", "identifier"), fe(null, "name"))),
        arguments(List.of("Patient.identifier", "name", "Observation.subject"),
            List.of(fe("Patient", "identifier"), fe(null, "name"), fe("Observation", "subject"))),
        arguments(List.of("Patient.identifier", "name", "Observation.identifier"),
            List.of(fe("Patient", "identifier"), fe(null, "name"),
                fe("Observation", "identifier"))),
        arguments(List.of("Patient2.identifier"), null),
        arguments(List.of("Patient.identifier", "Patient.not_real"), null),
        arguments(List.of("Patient.identifier", "Patient.subject"), null),
        arguments(List.of("null.identifier"), null)
    );
  }

  private static ExportRequest.FhirElement fe(String resourceType, String elementName) {
    return new ExportRequest.FhirElement(Enumerations.ResourceType.fromCode(resourceType),
        elementName);
  }

  @ParameterizedTest
  @MethodSource("provide_params")
  void test_params_with_lenient(ExportOutputFormat exportOutputFormat, InstantType since,
      List<String> type, Map<String, String[]> unsupportedQueryParams, boolean lenient,
      List<String> messages, boolean valid) {
    RequestDetails mockReqDetails = mockRequest("application/fhir+json", "respond-async", lenient);
    when(mockReqDetails.getParameters()).thenReturn(unsupportedQueryParams);
    String outputFormat = ExportOutputFormat.asParam(exportOutputFormat);

    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        PreAsyncValidationResult<ExportRequest> result = exportOperationValidator.validateRequest(
            mockReqDetails, outputFormat, since, null, type, List.of());
        assertThat(result)
            .isNotNull()
            .extracting(PreAsyncValidationResult::warnings, LIST)
            .map(OperationOutcome.OperationOutcomeIssueComponent.class::cast)
            .map(issue -> issue.getDetails().getText())
            .containsExactlyInAnyOrderElementsOf(messages);
      });
    } else {
      assertThatException().isThrownBy(
              () -> exportOperationValidator.validateRequest(mockReqDetails, outputFormat, since, null,
                  type, List.of()))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provide_params() {
    InstantType now = InstantType.now();
    return Stream.of(
        arguments(ND_JSON, now, List.<String>of(), Map.of(), false, List.of(), true),
        arguments(ND_JSON, now, List.of(""), Map.of(), false, List.of(), true),
        arguments(ND_JSON, now, List.of("Patient"), Map.of(), false, List.of(), true),
        arguments(ND_JSON, now, List.of("Patient123"), Map.of(), false, List.of(), false),
        arguments(ND_JSON, now, List.of("Patient123"), Map.of(), true, List.of(), true),
        arguments(ND_JSON, now, List.<String>of(), query_p("_typeFilter", "test"), false, List.of(),
            false),
        arguments(ND_JSON, now, List.<String>of(), query_p("_typeFilter", "test"), true, List.of(
                "The query parameter '_typeFilter' is not supported. Ignoring because lenient handling is enabled."),
            true)
    );
  }

  private static Map<String, String[]> query_p(String key1, String val1) {
    return Map.of(
        key1, new String[]{val1}
    );
  }

  @ParameterizedTest
  @MethodSource("provide_output_mappings")
  void test_output_model_mapping(ExportResponse exportResponse, JsonNode expectedJSON)
      throws IOException {
    Binary actualBinary = resolveTempDirIn(exportResponse, Paths.get("test"),
        UUID.randomUUID()).toOutput();
    assertThat(actualBinary.getContentType()).isEqualTo("application/json");
    JsonNode actualJSON = objectMapper.readTree(actualBinary.getContent());
    assertThat(actualJSON)
        .usingRecursiveComparison()
        .isEqualTo(expectedJSON);
  }

  private static Stream<Arguments> provide_output_mappings() {
    ObjectMapper mapper = new ObjectMapper();
    String base = "http://localhost:8080/fhir/$export?";
    InstantType now = InstantType.now();

    var req1 = req(base, ND_JSON, now, List.of(Enumerations.ResourceType.PATIENT));
    var res1 = res(req1,
        write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 1)));
    var json1 = json(mapper, res1.getKickOffRequestUrl(), res1.getWriteDetails());

    var req2 = req(base, ND_JSON, now, List.of(Enumerations.ResourceType.PATIENT));
    var res2 = res(req2,
        write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 0)));
    var json2 = json(mapper, res2.getKickOffRequestUrl(), write_details(
        res2.getWriteDetails().fileInfos().stream().filter(fileInfo -> fileInfo.count() > 0)
            .toList()));

    return Stream.of(
        arguments(res1, json1),
        arguments(res2, json2)
    );
  }

  @ParameterizedTest
  @MethodSource("provide_mappings")
  void test_input_model_mapping(String originalRequest, String outputFormat, InstantType since,
      InstantType until, List<String> type, ExportRequest expectedMappedRequest) {
    if (expectedMappedRequest == null) {
      assertThatException().isThrownBy(
              () -> exportOperationValidator.createExportRequest(originalRequest, false, outputFormat,
                  since, until, type, List.of()))
          .isExactlyInstanceOf(InvalidRequestException.class);
    } else {
      ExportRequest expectedRequest = exportOperationValidator.createExportRequest(originalRequest,
          false, outputFormat, since, until, type, List.of());
      assertThat(expectedRequest).isEqualTo(expectedMappedRequest);
    }
  }

  private static Stream<Arguments> provide_mappings() {
    String base = "http://localhost:8080/fhir/$export?";
    InstantType now = InstantType.now();
    InstantType until = InstantType.now();
    return Stream.of(
        arguments(base + "_outputFormat=application/fhir+ndjson", "application/fhir+ndjson", now,
            until, List.of(),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=application/fhir+ndjson")
                .outputFormat(ND_JSON)
                .since(now)
                .until(until)
                .build()
        ),
        arguments(base + "_outputFormat=application/ndjson", "application/ndjson", now, until,
            List.of(),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=application/ndjson")
                .outputFormat(ND_JSON)
                .since(now)
                .until(until)
                .build()
        ),
        arguments(base + "_outputFormat=ndjson", "ndjson", now, until, List.of(),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=ndjson")
                .outputFormat(ND_JSON)
                .since(now)
                .until(until)
                .build()
        ),
        arguments(base + "_outputFormat=abc", "abc", now, until, List.of(), null),
        arguments(base + "_outputFormat=ndjson", "ndjson", now, until, List.of("Patient"),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=ndjson")
                .outputFormat(ND_JSON)
                .since(now)
                .until(until)
                .includeResourceType(Enumerations.ResourceType.PATIENT)
                .build()
        ),
        arguments(base + "_outputFormat=ndjson", "ndjson", now, until,
            List.of("Patient", "Observation"),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=ndjson")
                .outputFormat(ND_JSON)
                .since(now)
                .until(until)
                .includeResourceTypes(Enumerations.ResourceType.PATIENT,
                    Enumerations.ResourceType.OBSERVATION)
                .build()
        ),
        arguments(base + "_outputFormat=ndjson", "ndjson", now, until, List.of("not_real"), null),
        arguments(base + "_outputFormat=ndjson", "ndjson", now, until,
            List.of("not_real1", "not_real2"), null)
    );
  }

  private static RequestDetails mockRequest(String acceptHeader, String preferHeader,
      boolean lenient) {
    RequestDetails details = mock(RequestDetails.class);
    when(details.getHeader("Accept")).thenReturn(acceptHeader);
    List<String> accept = new ArrayList<>();
    if (acceptHeader != null) {
      accept.add(acceptHeader);
    }
    when(details.getHeaders("Accept")).thenReturn(accept);
    when(details.getHeader("Prefer")).thenReturn(preferHeader);
    List<String> prefer = new ArrayList<>();
    if (preferHeader != null) {
      prefer.add(preferHeader);
    }
    if (lenient) {
      prefer.add("handling=lenient");
    }
    when(details.getHeaders("Prefer")).thenReturn(prefer);
    when(details.getCompleteUrl()).thenReturn("test-url");
    return details;
  }

}
