/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export.ws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.export.fhir.Parameters;
import au.csiro.pathling.export.fhir.Parameters.Parameter;
import au.csiro.pathling.export.fhir.Reference;
import au.csiro.pathling.export.ws.BulkExportRequest.GroupLevel;
import au.csiro.pathling.export.ws.BulkExportRequest.PatientLevel;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

class BulkExportRequestTest {
  
  @Test
  void testToParametersWithAllDefaults() {
    final BulkExportRequest request = BulkExportRequest.builder()
        .build();
    assertEquals(Parameters.of(), request.toParameters());
  }

  @Test
  void testToParametersWithWithAllValuesSet() {
    final BulkExportRequest request = BulkExportRequest.builder()
        ._outputFormat("fhir+ndjson")
        ._since(Instant.parse("2023-08-01T00:00:00Z"))
        ._type(List.of("Patient", "Condition"))
        ._elements(List.of("Patient.name", "Patient.birthDate"))
        ._typeFilter(List.of("Patient?active=true", "Condition?clinicalStatus=active"))
        .patient(List.of(Reference.of("Patient/00"), Reference.of("Patient/01")))
        .build();
    assertEquals(
        Parameters.of(
            Parameter.of("_outputFormat", "fhir+ndjson"),
            Parameter.of("_since", Instant.parse("2023-08-01T00:00:00Z")),
            Parameter.of("_type", "Patient,Condition"),
            Parameter.of("_elements", "Patient.name,Patient.birthDate"),
            Parameter.of("_typeFilter", "Patient?active=true,Condition?clinicalStatus=active"),
            Parameter.of("patient", Reference.of("Patient/00")),
            Parameter.of("patient", Reference.of("Patient/01"))
        ),
        request.toParameters());
  }

  @Test
  void testDefaultRequestUri() {
    final URI baseUri = URI.create("http://example.com/fhir");
    assertEquals(URI.create("http://example.com/fhir"),
        BulkExportRequest.builder().build().toRequestURI(baseUri)
    );
  }

  @Test
  void testNonDefaultRequestUri() {
    final URI baseUri = URI.create("http://test.com/fhir");
    final Instant testInstant = Instant.parse("2023-01-11T00:00:00.1234Z");
    assertEquals(URI.create(
            "http://test.com/fhir?_outputFormat=xml"
                + "&_since=2023-01-11T00%3A00%3A00.123Z"
                + "&_type=Patient%2CObservation"
                + "&_elements=Patient.id%2CCondition.status"
                + "&_typeFilter=Patient.active%3Dtrue%2CObservation.status%3Dfinal"),
        BulkExportRequest.builder()
            ._outputFormat("xml")
            ._type(List.of("Patient", "Observation"))
            ._elements(List.of("Patient.id", "Condition.status"))
            ._typeFilter(List.of("Patient.active=true", "Observation.status=final"))
            ._since(testInstant)
            .build().toRequestURI(baseUri)
    );
  }


  @Test
  void testCreateDefaultSystemExportRequest() {
    final BulkExportRequest request = BulkExportRequest.builder()
        .build();
    final HttpUriRequest httpRequest = request.toHttpRequest(URI.create("http://example.com/fhir"));
    assertEquals("GET", httpRequest.getMethod());
    assertEquals("http://example.com/fhir/$export",
        httpRequest.getURI().toString());
    assertEquals("application/fhir+json",
        httpRequest.getFirstHeader("accept").getValue());
    assertEquals("respond-async",
        httpRequest.getFirstHeader("prefer").getValue());
  }
  
  @Test
  void testCreatesAllValuesSystemExportRequest() {
    final Instant testInstant = Instant.parse("2023-01-11T00:00:00.1234Z");
    final BulkExportRequest request = BulkExportRequest.builder()
        ._outputFormat("xml")
        ._type(List.of("Patient", "Observation"))
        ._elements(List.of("Patient.id", "Condition.status"))
        ._typeFilter(List.of("Patient.active=true", "Observation.status=final"))
        ._since(testInstant)
        .build();
    final HttpUriRequest httpRequest = request.toHttpRequest(URI.create("http://test.com/fhir"));
    assertEquals("GET", httpRequest.getMethod());
    assertEquals("http://test.com/fhir/$export?_outputFormat=xml"
            + "&_since=2023-01-11T00%3A00%3A00.123Z"
            + "&_type=Patient%2CObservation"
            + "&_elements=Patient.id%2CCondition.status"
            + "&_typeFilter=Patient.active%3Dtrue%2CObservation.status%3Dfinal",
        httpRequest.getURI().toString());
    assertEquals("application/fhir+json",
        httpRequest.getFirstHeader("accept").getValue());
    assertEquals("respond-async",
        httpRequest.getFirstHeader("prefer").getValue());
  }
  
  @Test
  void testFailsOnSystemLevelWithPatientReferences() {
    final BulkExportRequest request = BulkExportRequest.builder()
        .patient(List.of(Reference.of("Patient/00")))
        .build();
    final IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> request.toHttpRequest(URI.create("http://example.com/fhir")));
    assertEquals("'patient' is not supported for operation: BulkExportRequest.SystemLevel()",
        ex.getMessage());

  }

  @Test
  void testCreatePatientLevelExportGetRequest() {
    final BulkExportRequest request = BulkExportRequest.builder()
        .level(new PatientLevel())
        ._type(List.of("Patient"))
        .build();
    final HttpUriRequest httpRequest = request.toHttpRequest(URI.create("http://example.com/fhir"));
    assertEquals("GET", httpRequest.getMethod());
    assertEquals("http://example.com/fhir/Patient/$export?_type=Patient",
        httpRequest.getURI().toString());
    assertEquals("application/fhir+json",
        httpRequest.getFirstHeader("accept").getValue());
    assertEquals("respond-async",
        httpRequest.getFirstHeader("prefer").getValue());
  }

  @Test
  void testCreatePatientLevelExportPostRequest() throws IOException {
    final BulkExportRequest request = BulkExportRequest.builder()
        .level(new PatientLevel())
        ._type(List.of("Patient"))
        .patient(List.of(Reference.of("Patient/00")))
        .build();
    final HttpUriRequest httpRequest = request.toHttpRequest(URI.create("http://example.com/fhir"));
    assertEquals("POST", httpRequest.getMethod());
    assertEquals("http://example.com/fhir/Patient/$export",
        httpRequest.getURI().toString());
    assertEquals("application/fhir+json",
        httpRequest.getFirstHeader("accept").getValue());
    assertEquals("respond-async",
        httpRequest.getFirstHeader("prefer").getValue());

    final HttpEntity postEntity = ((HttpPost) httpRequest).getEntity();
    assertEquals("application/fhir+json; charset=UTF-8", postEntity.getContentType().getValue());
    assertEquals(
        Parameters.of(
            Parameters.Parameter.of("_type", "Patient"),
            Parameters.Parameter.of("patient", Reference.of("Patient/00"))
        ).toJson(),
        EntityUtils.toString(postEntity));
  }
  
  @Test
  void testCreateGroupLevelExportGetRequest() {
    final BulkExportRequest request = BulkExportRequest.builder()
        .level(new GroupLevel("id0001"))
        ._outputFormat("xml")
        .build();
    final HttpUriRequest httpRequest = request.toHttpRequest(URI.create("http://example.com/fhir"));
    assertEquals("GET", httpRequest.getMethod());
    assertEquals("http://example.com/fhir/Group/id0001/$export?_outputFormat=xml",
        httpRequest.getURI().toString());
    assertEquals("application/fhir+json",
        httpRequest.getFirstHeader("accept").getValue());
    assertEquals("respond-async",
        httpRequest.getFirstHeader("prefer").getValue());
  }

  @Test
  void testCreateGroupLevelExportPostRequest() throws IOException {
    final BulkExportRequest request = BulkExportRequest.builder()
        .level(new GroupLevel("id0001"))
        ._outputFormat("fhir+ndjson")
        ._since(Instant.parse("2023-08-01T00:00:00Z"))
        ._type(List.of("Patient", "Condition"))
        ._elements(List.of("Patient.name", "Patient.birthDate"))
        ._typeFilter(List.of("Patient?active=true", "Condition?clinicalStatus=active"))
        .patient(List.of(Reference.of("Patient/00"), Reference.of("Patient/01")))
        .build();

    final HttpUriRequest httpRequest = request.toHttpRequest(URI.create("http://example.com/fhir"));
    assertEquals("POST", httpRequest.getMethod());
    assertEquals("http://example.com/fhir/Group/id0001/$export",
        httpRequest.getURI().toString());
    assertEquals("application/fhir+json",
        httpRequest.getFirstHeader("accept").getValue());
    assertEquals("respond-async",
        httpRequest.getFirstHeader("prefer").getValue());

    final HttpEntity postEntity = ((HttpPost) httpRequest).getEntity();
    assertEquals("application/fhir+json; charset=UTF-8", postEntity.getContentType().getValue());
    assertEquals(
        Parameters.of(
            Parameter.of("_outputFormat", "fhir+ndjson"),
            Parameter.of("_since", Instant.parse("2023-08-01T00:00:00Z")),
            Parameter.of("_type", "Patient,Condition"),
            Parameter.of("_elements", "Patient.name,Patient.birthDate"),
            Parameter.of("_typeFilter", "Patient?active=true,Condition?clinicalStatus=active"),
            Parameter.of("patient", Reference.of("Patient/00")),
            Parameter.of("patient", Reference.of("Patient/01"))
        ).toJson(),
        EntityUtils.toString(postEntity));
  }
}
