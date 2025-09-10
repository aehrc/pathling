package au.csiro.pathling;

import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.export.*;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.ExportRequestBuilder;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static au.csiro.pathling.async.PreAsyncValidation.*;
import static au.csiro.pathling.export.ExportOutputFormat.ND_JSON;
import static au.csiro.pathling.util.ExportOperationUtil.*;
import static au.csiro.pathling.util.TestConstants.RESOLVE_PATIENT;
import static au.csiro.pathling.util.TestConstants.WAREHOUSE_PLACEHOLDER;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Felix Naumann
 */

@Import({
        ExportOperationValidator.class,
        ExportExecutor.class,
        PathlingContext.class
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
    void test_headers(String acceptHeader, String preferHeader, boolean lenient, List<String> messages, boolean valid) {
        String outputFormat = ExportOutputFormat.asParam(ND_JSON);
        InstantType now = InstantType.now();
        if(valid) {
            assertThatNoException().isThrownBy(() -> {
                PreAsyncValidationResult<ExportRequest> result = exportOperationValidator.validateRequest(mockRequest(acceptHeader, preferHeader, lenient), outputFormat, now, null, null, null);
                assertThat(result)
                        .isNotNull()
                        .extracting(PreAsyncValidationResult::warnings, LIST)
                        .map(OperationOutcome.OperationOutcomeIssueComponent.class::cast)
                        .map(issue -> issue.getDetails().getText())
                        .containsExactlyInAnyOrderElementsOf(messages);
            });
        }
        else {
            assertThatException().isThrownBy(() -> exportOperationValidator.validateRequest(mockRequest(acceptHeader, preferHeader, lenient), outputFormat, now, null, null, null))
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
                arguments("application/fhir+ndjson", "respond-async", true, List.of("Added missing header: Accept application/fhir+json"), true),
                arguments("ndjson", "respond-async", true, List.of("Added missing header: Accept application/fhir+json"), true),
                arguments(null, "respond-async", true, List.of("Added missing header: Accept application/fhir+json"), true),
                arguments(null, null, true, List.of("Added missing header: Accept application/fhir+json", "Added missing header: Prefer respond-async"), true),
                arguments("application/fhir+json", null, true, List.of("Added missing header: Prefer respond-async"), true),
                arguments("application/fhir+json", "abc", true, List.of("Added missing header: Prefer respond-async"), true)

        );
    }

    @ParameterizedTest
    @MethodSource("provide_elements_param")
    void test_elements_param_is_mapped(List<String> elements, List<ExportRequest.FhirElement> expectedElements) {
        RequestDetails mockReqDetails = mockRequest("application/fhir+json", "respond-async", false);
        ExportRequest actualExportRequest = exportOperationValidator.validateRequest(mockReqDetails, ExportOutputFormat.asParam(ND_JSON), InstantType.now(), null, null, elements).result();
        assertThat(actualExportRequest).isNotNull();
        assertThat(actualExportRequest.elements()).containsExactlyInAnyOrderElementsOf(expectedElements);
    }

    private static Stream<Arguments> provide_elements_param() {
        return Stream.of(
                arguments(List.of("Patient.identifier"), List.of(fe("Patient", "identifier"))),
                arguments(List.of("identifier"), List.of(fe(null, "identifier"))),
                arguments(List.of("Patient.identifier", "name"), List.of(fe("Patient", "identifier"), fe(null, "name"))),
                arguments(List.of("Patient.identifier", "name", "Observation.subject"), List.of(fe("Patient", "identifier"), fe(null, "name"), fe("Observation", "subject"))),
                arguments(List.of("Patient.identifier", "name", "Observation.identifier"), List.of(fe("Patient", "identifier"), fe(null, "name"), fe("Observation", "identifier")))
        );
    }

    private static ExportRequest.FhirElement fe(String resourceType, String elementName) {
        return new ExportRequest.FhirElement(Enumerations.ResourceType.fromCode(resourceType), elementName);
    }

    @ParameterizedTest
    @MethodSource("provide_params")
    void test_params_with_lenient(ExportOutputFormat exportOutputFormat, InstantType since, List<String> type, Map<String, String[]> unsupportedQueryParams, boolean lenient, List<String> messages, boolean valid) {
        RequestDetails mockReqDetails = mockRequest("application/fhir+json", "respond-async", lenient);
        when(mockReqDetails.getParameters()).thenReturn(unsupportedQueryParams);
        String outputFormat = ExportOutputFormat.asParam(exportOutputFormat);

        if(valid) {
            assertThatNoException().isThrownBy(() -> {
                PreAsyncValidationResult<ExportRequest> result = exportOperationValidator.validateRequest(mockReqDetails, outputFormat, since, null, type, List.of());
                assertThat(result)
                        .isNotNull()
                        .extracting(PreAsyncValidationResult::warnings, LIST)
                        .map(OperationOutcome.OperationOutcomeIssueComponent.class::cast)
                        .map(issue -> issue.getDetails().getText())
                        .containsExactlyInAnyOrderElementsOf(messages);
            });
        }
        else {
            assertThatException().isThrownBy(() -> exportOperationValidator.validateRequest(mockReqDetails, outputFormat, since, null, type, List.of()))
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
                arguments(ND_JSON, now, List.of("Patient123"), Map.of(), true, List.of(), false),
                arguments(ND_JSON, now, List.<String>of(), query_p("_typeFilter", "test"), false, List.of(), false),
                arguments(ND_JSON, now, List.<String>of(), query_p("_typeFilter", "test"), true, List.of("The query parameter '_typeFilter' is not supported. Ignoring because lenient handling is enabled."), true)
        );
    }

    private static Map<String, String[]> query_p(String key1, String val1) {
        return Map.of(
                key1, new String[] { val1 }
        );
    }

    private static Map<String, String[]> query_p(String key1, String val1, String key2, String val2) {
        return Map.of(
                key1, new String[] { val1 },
                key2, new String[] { val2 }
        );
    }

    @ParameterizedTest
    @MethodSource("provide_output_mappings")
    void test_output_model_mapping(ExportResponse exportResponse, JsonNode expectedJSON) throws IOException {
        Binary actualBinary = exportResponse.toOutput();
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
        var res1 =  res(req1, write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 1)));
        var json1 = json(mapper, res1.getKickOffRequestUrl(), res1.getWriteDetails());

        var req2 = req(base, ND_JSON, now, List.of(Enumerations.ResourceType.PATIENT));
        var res2 =  res(req2, write_details(fi("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER), 0)));
        var json2 = json(mapper, res2.getKickOffRequestUrl(), write_details(res2.getWriteDetails().fileInfos().stream().filter(fileInfo -> fileInfo.count() > 0).toList()));

        return Stream.of(
                arguments(res1, json1),
                arguments(res2, json2)
        );
    }

    @ParameterizedTest
    @MethodSource("provide_mappings")
    public void test_input_model_mapping(String originalRequest, String outputFormat, InstantType since, List<String> type, ExportRequest expectedMappedRequest) {
        if(expectedMappedRequest == null) {
            assertThatException().isThrownBy(() -> exportOperationValidator.createExportRequest(originalRequest, outputFormat, since, null, type, List.of()))
                    .isExactlyInstanceOf(InvalidRequestException.class);
        }
        else {
            ExportRequest expectedRequest = exportOperationValidator.createExportRequest(originalRequest, outputFormat, since, null, type, List.of());
            assertThat(expectedRequest).isEqualTo(expectedMappedRequest);
        }
    }

    private static Stream<Arguments> provide_mappings() {
        String base = "http://localhost:8080/fhir/$export?";
        InstantType now = InstantType.now();
        return Stream.of(
                arguments(base + "_outputFormat=application/fhir+ndjson", "application/fhir+ndjson", now, List.of(),
                        ExportRequestBuilder.builder()
                                .originalRequest(base + "_outputFormat=application/fhir+ndjson")
                                .outputFormat(ND_JSON)
                                .since(now)
                                .build()
                ),
                arguments(base + "_outputFormat=application/ndjson", "application/ndjson", now, List.of(),
                        ExportRequestBuilder.builder()
                                .originalRequest(base + "_outputFormat=application/ndjson")
                                .outputFormat(ND_JSON)
                                .since(now)
                                .build()
                ),
                arguments(base + "_outputFormat=ndjson", "ndjson", now, List.of(),
                        ExportRequestBuilder.builder()
                                .originalRequest(base + "_outputFormat=ndjson")
                                .outputFormat(ND_JSON)
                                .since(now)
                                .build()
                ),
                arguments(base + "_outputFormat=abc", "abc", now, List.of(), null),
                arguments(base + "_outputFormat=ndjson", "ndjson", now, List.of("Patient"),
                        ExportRequestBuilder.builder()
                                .originalRequest(base + "_outputFormat=ndjson")
                                .outputFormat(ND_JSON)
                                .since(now)
                                .includeResourceType(Enumerations.ResourceType.PATIENT)
                                .build()
                ),
                arguments(base + "_outputFormat=ndjson", "ndjson", now, List.of("Patient", "Observation"),
                        ExportRequestBuilder.builder()
                                .originalRequest(base + "_outputFormat=ndjson")
                                .outputFormat(ND_JSON)
                                .since(now)
                                .includeResourceTypes(Enumerations.ResourceType.PATIENT, Enumerations.ResourceType.OBSERVATION)
                                .build()
                ),
                arguments(base + "_outputFormat=ndjson", "ndjson", now, List.of("not_real"), null),
                arguments(base + "_outputFormat=ndjson", "ndjson", now, List.of("not_real1", "not_real2"), null)
        );
    }

    private static RequestDetails mockRequest(String acceptHeader, String preferHeader) {
        return mockRequest(acceptHeader, preferHeader, false);
    }

    private static RequestDetails mockRequest(String acceptHeader, String preferHeader, boolean lenient) {
        RequestDetails details = mock(RequestDetails.class);
        when(details.getHeader("Accept")).thenReturn(acceptHeader);
        List<String> accept = new ArrayList<>();
        if(acceptHeader != null) {
            accept.add(acceptHeader);
        }
        when(details.getHeaders("Accept")).thenReturn(accept);
        when(details.getHeader("Prefer")).thenReturn(preferHeader);
        List<String> prefer = new ArrayList<>();
        if(preferHeader != null) {
            prefer.add(preferHeader);
        }
        if(lenient) {
            prefer.add("handling=lenient");
        }
        when(details.getHeaders("Prefer")).thenReturn(prefer);
        return details;
    }

}
