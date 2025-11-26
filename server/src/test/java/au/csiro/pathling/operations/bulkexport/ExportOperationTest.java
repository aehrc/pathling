package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import static au.csiro.pathling.operations.bulkexport.ExportOutputFormat.ND_JSON;
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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.ExportRequestBuilder;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
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
import scala.jdk.javaapi.CollectionConverters;

/**
 * @author Felix Naumann
 */

@Import({
    ExportOperationValidator.class,
    ExportExecutor.class,
    FhirServerTestConfiguration.class,
    au.csiro.pathling.operations.compartment.PatientCompartmentService.class
})
@SpringBootUnitTest
class ExportOperationTest {

  @Autowired
  private ExportOperationValidator exportOperationValidator;

  private ObjectMapper objectMapper;

  @SuppressWarnings("unused")
  @MockBean
  private QueryableDataSource queryableDataSource; // NO-SONAR the mock is needed

  @BeforeEach
  void initialize() {
    SharedMocks.resetAll();
    objectMapper = new ObjectMapper();
  }

  @ParameterizedTest
  @MethodSource("provideHeaders")
  void testHeaders(final String acceptHeader, final String preferHeader, final boolean lenient,
      final List<String> messages, final boolean valid) {
    final String outputFormat = ExportOutputFormat.asParam(ND_JSON);
    final InstantType now = InstantType.now();
    final RequestDetails mockRequest = MockUtil.mockRequest(acceptHeader, preferHeader, lenient);
    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        final PreAsyncValidationResult<ExportRequest> result = exportOperationValidator.validateRequest(
            mockRequest, outputFormat, now, null, null, null);
        assertThat(result)
            .isNotNull()
            .extracting(PreAsyncValidationResult::warnings, LIST)
            .map(OperationOutcome.OperationOutcomeIssueComponent.class::cast)
            .map(issue -> issue.getDetails().getText())
            .containsExactlyInAnyOrderElementsOf(messages);
      });
    } else {
      assertThatThrownBy(
          () -> exportOperationValidator.validateRequest(mockRequest, outputFormat, now, null, null,
              null)).isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provideHeaders() {
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
  @MethodSource("provideUntilParameter")
  void testUntilParameterIsMapped(final InstantType until, final InstantType expectedUntil) {
    final RequestDetails mockReqDetails = MockUtil.mockRequest("application/fhir+json",
        "respond-async",
        false);
    final ExportRequest actualExportRequest = exportOperationValidator.validateRequest(
        mockReqDetails,
        ExportOutputFormat.asParam(ND_JSON), InstantType.now(), until, null, null).result();
    assertThat(actualExportRequest).isNotNull();
    assertThat(actualExportRequest.until()).isEqualTo(expectedUntil);
  }

  private static Stream<Arguments> provideUntilParameter() {
    final InstantType now = InstantType.now();
    return Stream.of(
        arguments(now, now),
        arguments(null, null)
    );
  }

  @ParameterizedTest
  @MethodSource("provideTypeParameter")
  void testTypeParameterIsMapped(final List<String> types, final boolean lenient,
      final List<ResourceType> expectedTypes) {
    final RequestDetails mockReqDetails = MockUtil.mockRequest("application/fhir+json",
        "respond-async",
        lenient);
    final InstantType now = InstantType.now();
    if (expectedTypes != null) {
      final ExportRequest actualExportRequest = exportOperationValidator.validateRequest(
          mockReqDetails,
          ExportOutputFormat.asParam(ND_JSON), now, null, types, null).result();
      assertThat(actualExportRequest).isNotNull();
      assertThat(
          actualExportRequest.includeResourceTypeFilters()).containsExactlyInAnyOrderElementsOf(
          expectedTypes);
    } else {
      final String outputFormat = ExportOutputFormat.asParam(ND_JSON);
      assertThatCode(
          () -> exportOperationValidator.validateRequest(mockReqDetails, outputFormat, now, null,
              types, null))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provideTypeParameter() {
    final String unsupportedResource = CollectionConverters.asJava(
            EncoderBuilder.UNSUPPORTED_RESOURCES())
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
  @MethodSource("provideElementsParameter")
  void testElementsParameterIsMapped(final List<String> elements,
      final List<ExportRequest.FhirElement> expectedElements) {
    final RequestDetails mockReqDetails = MockUtil.mockRequest("application/fhir+json",
        "respond-async",
        false);
    final String outputFormat = ExportOutputFormat.asParam(ND_JSON);
    final InstantType now = InstantType.now();
    if (expectedElements != null) {
      final ExportRequest actualExportRequest = exportOperationValidator.validateRequest(
          mockReqDetails,
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

  private static Stream<Arguments> provideElementsParameter() {
    return Stream.of(
        arguments(List.of("Patient.identifier"), List.of(fhirElement("Patient", "identifier"))),
        arguments(List.of("identifier"), List.of(fhirElement(null, "identifier"))),
        arguments(List.of("Patient.identifier", "name"),
            List.of(fhirElement("Patient", "identifier"), fhirElement(null, "name"))),
        arguments(List.of("Patient.identifier", "name", "Observation.subject"),
            List.of(fhirElement("Patient", "identifier"), fhirElement(null, "name"),
                fhirElement("Observation", "subject"))),
        arguments(List.of("Patient.identifier", "name", "Observation.identifier"),
            List.of(fhirElement("Patient", "identifier"), fhirElement(null, "name"),
                fhirElement("Observation", "identifier"))),
        arguments(List.of("Patient2.identifier"), null),
        arguments(List.of("Patient.identifier", "Patient.not_real"), null),
        arguments(List.of("Patient.identifier", "Patient.subject"), null),
        arguments(List.of("null.identifier"), null)
    );
  }

  private static ExportRequest.FhirElement fhirElement(@Nullable final String resourceType,
      final String elementName) {
    return new ExportRequest.FhirElement(Enumerations.ResourceType.fromCode(resourceType),
        elementName);
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  void testParametersWithLenient(final ExportOutputFormat exportOutputFormat,
      final InstantType since, final List<String> type,
      final Map<String, String[]> unsupportedQueryParams, final boolean lenient,
      final List<String> messages, final boolean valid) {
    final RequestDetails mockReqDetails = MockUtil.mockRequest("application/fhir+json",
        "respond-async",
        lenient);
    when(mockReqDetails.getParameters()).thenReturn(unsupportedQueryParams);
    final String outputFormat = ExportOutputFormat.asParam(exportOutputFormat);

    final List<String> emptyList = List.of();
    if (valid) {
      assertThatNoException().isThrownBy(() -> {
        final PreAsyncValidationResult<ExportRequest> result = exportOperationValidator.validateRequest(
            mockReqDetails, outputFormat, since, null, type, emptyList);
        assertThat(result)
            .isNotNull()
            .extracting(PreAsyncValidationResult::warnings, LIST)
            .map(OperationOutcome.OperationOutcomeIssueComponent.class::cast)
            .map(issue -> issue.getDetails().getText())
            .containsExactlyInAnyOrderElementsOf(messages);
      });
    } else {
      assertThatThrownBy(
          () -> exportOperationValidator.validateRequest(mockReqDetails, outputFormat, since, null,
              type, emptyList))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provideParameters() {
    final InstantType now = InstantType.now();
    return Stream.of(
        arguments(ND_JSON, now, List.<String>of(), Map.of(), false, List.of(), true),
        arguments(ND_JSON, now, List.of(""), Map.of(), false, List.of(), true),
        arguments(ND_JSON, now, List.of("Patient"), Map.of(), false, List.of(), true),
        arguments(ND_JSON, now, List.of("Patient123"), Map.of(), false, List.of(), false),
        arguments(ND_JSON, now, List.of("Patient123"), Map.of(), true, List.of(), true),
        arguments(ND_JSON, now, List.<String>of(), queryParameter("_typeFilter", "test"), false,
            List.of(),
            false),
        arguments(ND_JSON, now, List.<String>of(), queryParameter("_typeFilter", "test"), true,
            List.of(
                "The query parameter '_typeFilter' is not supported. Ignoring because lenient handling is enabled."),
            true)
    );
  }

  @SuppressWarnings("SameParameterValue")
  private static Map<String, String[]> queryParameter(final String key1, final String val1) {
    return Map.of(
        key1, new String[]{val1}
    );
  }

  @ParameterizedTest
  @MethodSource("provideOutputMappings")
  void testOutputModelMapping(final ExportResponse exportResponse, final JsonNode expectedJSON)
      throws IOException {
    final Binary actualBinary = resolveTempDirIn(exportResponse, Paths.get("test"),
        UUID.randomUUID()).toOutput();
    assertThat(actualBinary.getContentType()).isEqualTo("application/json");
    final JsonNode actualJSON = objectMapper.readTree(actualBinary.getContent());
    assertThat(actualJSON)
        .usingRecursiveComparison()
        .isEqualTo(expectedJSON);
  }

  private static Stream<Arguments> provideOutputMappings() {
    final ObjectMapper mapper = new ObjectMapper();
    final String base = "http://localhost:8080/fhir/$export?";
    final InstantType now = InstantType.now();

    final var req1 = req(base, ND_JSON, now, List.of(Enumerations.ResourceType.PATIENT));
    final var res1 = res(req1,
        write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 1)));
    final var json1 = json(mapper, res1.getKickOffRequestUrl(), res1.getWriteDetails());

    final var req2 = req(base, ND_JSON, now, List.of(Enumerations.ResourceType.PATIENT));
    final var res2 = res(req2,
        write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 0)));
    final var json2 = json(mapper, res2.getKickOffRequestUrl(), write_details(
        res2.getWriteDetails().fileInfos().stream()
            .filter(fileInfo -> fileInfo.count() != null && fileInfo.count() > 0)
            .toList()));

    return Stream.of(
        arguments(res1, json1),
        arguments(res2, json2)
    );
  }

  @ParameterizedTest
  @MethodSource("provideMappings")
  void testInputModelMapping(final String originalRequest, final String outputFormat,
      final InstantType since, final InstantType until, final List<String> type,
      final ExportRequest expectedMappedRequest) {
    final List<String> emptyList = List.of();
    final String serverBaseUrl = "http://localhost:8080/fhir";
    if (expectedMappedRequest == null) {
      assertThatThrownBy(
          () -> exportOperationValidator.createExportRequest(originalRequest, serverBaseUrl, false,
              outputFormat, since, until, type, emptyList))
          .isExactlyInstanceOf(InvalidRequestException.class);
    } else {
      final ExportRequest expectedRequest = exportOperationValidator.createExportRequest(
          originalRequest, serverBaseUrl, false, outputFormat, since, until, type, emptyList);
      assertThat(expectedRequest).isEqualTo(expectedMappedRequest);
    }
  }

  private static Stream<Arguments> provideMappings() {
    final String base = "http://localhost:8080/fhir/$export?";
    final InstantType now = InstantType.now();
    final InstantType until = InstantType.now();
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

}
