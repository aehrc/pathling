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

package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import static au.csiro.pathling.operations.bulkexport.ExportOutputFormat.NDJSON;
import static au.csiro.pathling.util.ExportOperationUtil.fileInfo;
import static au.csiro.pathling.util.ExportOperationUtil.req;
import static au.csiro.pathling.util.ExportOperationUtil.res;
import static au.csiro.pathling.util.ExportOperationUtil.resolveTempDirIn;
import static au.csiro.pathling.util.ExportOperationUtil.writeDetails;
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
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.ExportRequestBuilder;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nullable;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
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

  @Autowired private ExportOperationValidator exportOperationValidator;

  private ObjectMapper objectMapper;

  @SuppressWarnings("unused")
  @MockitoBean
  private QueryableDataSource queryableDataSource; // NO-SONAR the mock is needed

  @BeforeEach
  void initialize() {
    SharedMocks.resetAll();
    objectMapper = new ObjectMapper();
  }

  @ParameterizedTest
  @MethodSource("provideHeaders")
  void testHeaders(
      final String acceptHeader,
      final String preferHeader,
      final boolean lenient,
      final List<String> messages,
      final boolean valid) {
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType now = InstantType.now();
    final RequestDetails mockRequest = MockUtil.mockRequest(acceptHeader, preferHeader, lenient);
    if (valid) {
      assertThatNoException()
          .isThrownBy(
              () -> {
                final PreAsyncValidationResult<ExportRequest> result =
                    exportOperationValidator.validateRequest(
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
              () ->
                  exportOperationValidator.validateRequest(
                      mockRequest, outputFormat, now, null, null, null))
          .isExactlyInstanceOf(InvalidRequestException.class);
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
        arguments(
            "application/fhir+ndjson",
            "respond-async",
            true,
            List.of("Added missing header: Accept application/fhir+json"),
            true),
        arguments(
            "ndjson",
            "respond-async",
            true,
            List.of("Added missing header: Accept application/fhir+json"),
            true),
        arguments(
            null,
            "respond-async",
            true,
            List.of("Added missing header: Accept application/fhir+json"),
            true),
        arguments(
            null,
            null,
            true,
            List.of(
                "Added missing header: Accept application/fhir+json",
                "Added missing header: Prefer respond-async"),
            true),
        arguments(
            "application/fhir+json",
            null,
            true,
            List.of("Added missing header: Prefer respond-async"),
            true),
        arguments(
            "application/fhir+json",
            "abc",
            true,
            List.of("Added missing header: Prefer respond-async"),
            true));
  }

  @ParameterizedTest
  @MethodSource("provideUntilParameter")
  void testUntilParameterIsMapped(final InstantType until, final InstantType expectedUntil) {
    final RequestDetails mockReqDetails =
        MockUtil.mockRequest("application/fhir+json", "respond-async", false);
    final ExportRequest actualExportRequest =
        exportOperationValidator
            .validateRequest(
                mockReqDetails,
                ExportOutputFormat.asParam(NDJSON),
                InstantType.now(),
                until,
                null,
                null)
            .result();
    assertThat(actualExportRequest).isNotNull();
    assertThat(actualExportRequest.until()).isEqualTo(expectedUntil);
  }

  private static Stream<Arguments> provideUntilParameter() {
    final InstantType now = InstantType.now();
    return Stream.of(arguments(now, now), arguments(null, null));
  }

  @ParameterizedTest
  @MethodSource("provideTypeParameter")
  void testTypeParameterIsMapped(
      final List<String> types, final boolean lenient, final List<String> expectedTypes) {
    final RequestDetails mockReqDetails =
        MockUtil.mockRequest("application/fhir+json", "respond-async", lenient);
    final InstantType now = InstantType.now();
    if (expectedTypes != null) {
      final ExportRequest actualExportRequest =
          exportOperationValidator
              .validateRequest(
                  mockReqDetails, ExportOutputFormat.asParam(NDJSON), now, null, types, null)
              .result();
      assertThat(actualExportRequest).isNotNull();
      assertThat(actualExportRequest.includeResourceTypeFilters())
          .containsExactlyInAnyOrderElementsOf(expectedTypes);
    } else {
      final String outputFormat = ExportOutputFormat.asParam(NDJSON);
      assertThatCode(
              () ->
                  exportOperationValidator.validateRequest(
                      mockReqDetails, outputFormat, now, null, types, null))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provideTypeParameter() {
    final String unsupportedResource =
        CollectionConverters.asJava(EncoderBuilder.UNSUPPORTED_RESOURCES()).stream()
            .findFirst()
            .orElseThrow();
    return Stream.of(
        arguments(List.of("Patient"), false, List.of("Patient")),
        arguments(List.of("Patient", "Observation"), false, List.of("Patient", "Observation")),
        arguments(List.of("Patient2", "Observation"), false, null),
        // invalid resource type and lenient=false -> error
        arguments(List.of("Patient2", "Observation"), true, List.of("Observation")),
        // invalid resource type and lenient=true -> pass but ignore invalid resource
        arguments(List.of(unsupportedResource, "Patient"), true, List.of("Patient")),
        arguments(List.of(unsupportedResource, "Patient"), false, null),
        arguments(List.of("NULL", "Patient"), false, null),
        arguments(List.of("NULL", "Patient"), true, List.of("Patient")));
  }

  @ParameterizedTest
  @MethodSource("provideElementsParameter")
  void testElementsParameterIsMapped(
      final List<String> elements, final List<ExportRequest.FhirElement> expectedElements) {
    final RequestDetails mockReqDetails =
        MockUtil.mockRequest("application/fhir+json", "respond-async", false);
    final String outputFormat = ExportOutputFormat.asParam(NDJSON);
    final InstantType now = InstantType.now();
    if (expectedElements != null) {
      final ExportRequest actualExportRequest =
          exportOperationValidator
              .validateRequest(mockReqDetails, outputFormat, now, null, null, elements)
              .result();
      assertThat(actualExportRequest).isNotNull();
      assertThat(actualExportRequest.elements())
          .containsExactlyInAnyOrderElementsOf(expectedElements);
    } else {
      assertThatCode(
              () ->
                  exportOperationValidator.validateRequest(
                      mockReqDetails, outputFormat, now, null, null, elements))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provideElementsParameter() {
    return Stream.of(
        arguments(List.of("Patient.identifier"), List.of(fhirElement("Patient", "identifier"))),
        arguments(List.of("identifier"), List.of(fhirElement(null, "identifier"))),
        arguments(
            List.of("Patient.identifier", "name"),
            List.of(fhirElement("Patient", "identifier"), fhirElement(null, "name"))),
        arguments(
            List.of("Patient.identifier", "name", "Observation.subject"),
            List.of(
                fhirElement("Patient", "identifier"),
                fhirElement(null, "name"),
                fhirElement("Observation", "subject"))),
        arguments(
            List.of("Patient.identifier", "name", "Observation.identifier"),
            List.of(
                fhirElement("Patient", "identifier"),
                fhirElement(null, "name"),
                fhirElement("Observation", "identifier"))),
        arguments(List.of("Patient2.identifier"), null),
        arguments(List.of("Patient.identifier", "Patient.not_real"), null),
        arguments(List.of("Patient.identifier", "Patient.subject"), null),
        arguments(List.of("null.identifier"), null));
  }

  private static ExportRequest.FhirElement fhirElement(
      @Nullable final String resourceTypeCode, final String elementName) {
    return new ExportRequest.FhirElement(resourceTypeCode, elementName);
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  void testParametersWithLenient(
      final ExportOutputFormat exportOutputFormat,
      final InstantType since,
      final List<String> type,
      final Map<String, String[]> unsupportedQueryParams,
      final boolean lenient,
      final List<String> messages,
      final boolean valid) {
    final RequestDetails mockReqDetails =
        MockUtil.mockRequest("application/fhir+json", "respond-async", lenient);
    when(mockReqDetails.getParameters()).thenReturn(unsupportedQueryParams);
    final String outputFormat = ExportOutputFormat.asParam(exportOutputFormat);

    final List<String> emptyList = List.of();
    if (valid) {
      assertThatNoException()
          .isThrownBy(
              () -> {
                final PreAsyncValidationResult<ExportRequest> result =
                    exportOperationValidator.validateRequest(
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
              () ->
                  exportOperationValidator.validateRequest(
                      mockReqDetails, outputFormat, since, null, type, emptyList))
          .isExactlyInstanceOf(InvalidRequestException.class);
    }
  }

  private static Stream<Arguments> provideParameters() {
    final InstantType now = InstantType.now();
    return Stream.of(
        arguments(NDJSON, now, List.<String>of(), Map.of(), false, List.of(), true),
        arguments(NDJSON, now, List.of(""), Map.of(), false, List.of(), true),
        arguments(NDJSON, now, List.of("Patient"), Map.of(), false, List.of(), true),
        arguments(NDJSON, now, List.of("Patient123"), Map.of(), false, List.of(), false),
        arguments(NDJSON, now, List.of("Patient123"), Map.of(), true, List.of(), true),
        arguments(
            NDJSON,
            now,
            List.<String>of(),
            queryParameter("_typeFilter", "test"),
            false,
            List.of(),
            false),
        arguments(
            NDJSON,
            now,
            List.<String>of(),
            queryParameter("_typeFilter", "test"),
            true,
            List.of(
                "The query parameter '_typeFilter' is not supported. Ignoring because lenient"
                    + " handling is enabled."),
            true));
  }

  @SuppressWarnings("SameParameterValue")
  private static Map<String, String[]> queryParameter(final String key1, final String val1) {
    return Map.of(key1, new String[] {val1});
  }

  @ParameterizedTest
  @MethodSource("provideOutputMappings")
  void testOutputModelMapping(
      final ExportResponse exportResponse,
      final String expectedRequest,
      final int expectedOutputs) {
    final Parameters actualParameters =
        resolveTempDirIn(exportResponse, Paths.get("test"), UUID.randomUUID()).toOutput();

    // Verify the Parameters structure.
    assertThat(actualParameters.getParameter()).hasSizeGreaterThanOrEqualTo(4);

    // Verify request parameter.
    final ParametersParameterComponent requestParam = findParameter(actualParameters, "request");
    assertThat(requestParam.getValue().primitiveValue()).isEqualTo(expectedRequest);

    // Verify requiresAccessToken parameter.
    final ParametersParameterComponent tokenParam =
        findParameter(actualParameters, "requiresAccessToken");
    assertThat(tokenParam.getValue().primitiveValue()).isEqualTo("false");

    // Verify transactionTime parameter exists.
    final ParametersParameterComponent timeParam =
        findParameter(actualParameters, "transactionTime");
    assertThat(timeParam.getValue()).isNotNull();

    // Verify output parameters.
    final List<ParametersParameterComponent> outputParams =
        actualParameters.getParameter().stream().filter(p -> "output".equals(p.getName())).toList();
    assertThat(outputParams).hasSize(expectedOutputs);
  }

  private static ParametersParameterComponent findParameter(
      final Parameters parameters, final String name) {
    return parameters.getParameter().stream()
        .filter(p -> name.equals(p.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Parameter not found: " + name));
  }

  private static Stream<Arguments> provideOutputMappings() {
    final String base = "http://localhost:8080/fhir/$export?";
    final InstantType now = InstantType.now();

    final var req1 = req(base, NDJSON, now, List.of("Patient"));
    final var res1 =
        res(req1, writeDetails(fileInfo("Patient", RESOLVE_PATIENT.apply(WAREHOUSE_PLACEHOLDER))));

    return Stream.of(arguments(res1, res1.getKickOffRequestUrl(), 1));
  }

  @ParameterizedTest
  @MethodSource("provideMappings")
  void testInputModelMapping(
      final String originalRequest,
      final String outputFormat,
      final InstantType since,
      final InstantType until,
      final List<String> type,
      final ExportRequest expectedMappedRequest) {
    final List<String> emptyList = List.of();
    final String serverBaseUrl = "http://localhost:8080/fhir";
    if (expectedMappedRequest == null) {
      assertThatThrownBy(
              () ->
                  exportOperationValidator.createExportRequest(
                      originalRequest,
                      serverBaseUrl,
                      false,
                      outputFormat,
                      since,
                      until,
                      type,
                      emptyList))
          .isExactlyInstanceOf(InvalidRequestException.class);
    } else {
      final ExportRequest expectedRequest =
          exportOperationValidator.createExportRequest(
              originalRequest, serverBaseUrl, false, outputFormat, since, until, type, emptyList);
      assertThat(expectedRequest).isEqualTo(expectedMappedRequest);
    }
  }

  private static Stream<Arguments> provideMappings() {
    final String base = "http://localhost:8080/fhir/$export?";
    final InstantType now = InstantType.now();
    final InstantType until = InstantType.now();
    return Stream.of(
        arguments(
            base + "_outputFormat=application/fhir+ndjson",
            "application/fhir+ndjson",
            now,
            until,
            List.of(),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=application/fhir+ndjson")
                .outputFormat(NDJSON)
                .since(now)
                .until(until)
                .build()),
        arguments(
            base + "_outputFormat=application/ndjson",
            "application/ndjson",
            now,
            until,
            List.of(),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=application/ndjson")
                .outputFormat(NDJSON)
                .since(now)
                .until(until)
                .build()),
        arguments(
            base + "_outputFormat=ndjson",
            "ndjson",
            now,
            until,
            List.of(),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=ndjson")
                .outputFormat(NDJSON)
                .since(now)
                .until(until)
                .build()),
        arguments(base + "_outputFormat=abc", "abc", now, until, List.of(), null),
        arguments(
            base + "_outputFormat=ndjson",
            "ndjson",
            now,
            until,
            List.of("Patient"),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=ndjson")
                .outputFormat(NDJSON)
                .since(now)
                .until(until)
                .includeResourceType("Patient")
                .build()),
        arguments(
            base + "_outputFormat=ndjson",
            "ndjson",
            now,
            until,
            List.of("Patient", "Observation"),
            ExportRequestBuilder.builder()
                .originalRequest(base + "_outputFormat=ndjson")
                .outputFormat(NDJSON)
                .since(now)
                .until(until)
                .includeResourceTypes("Patient", "Observation")
                .build()),
        arguments(base + "_outputFormat=ndjson", "ndjson", now, until, List.of("not_real"), null),
        arguments(
            base + "_outputFormat=ndjson",
            "ndjson",
            now,
            until,
            List.of("not_real1", "not_real2"),
            null));
  }
}
