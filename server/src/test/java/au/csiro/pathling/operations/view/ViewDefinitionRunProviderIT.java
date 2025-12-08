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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.springframework.http.MediaType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration tests for {@link ViewDefinitionRunProvider}.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class ViewDefinitionRunProviderIT {

  @LocalServerPort
  int port;

  @Autowired
  WebTestClient webTestClient;

  private static Path warehouseDir;

  @Autowired
  private FhirContext fhirContext;

  private Gson gson;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    warehouseDir = Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  @BeforeEach
  void setup() {
    webTestClient = webTestClient.mutate()
        .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
        .build();
    gson = new GsonBuilder().create();
  }

  @Test
  void ndjsonOutputWithInlineResources() {
    final String viewJson = createSimplePatientView();
    final Patient patient = new Patient();
    patient.setId("ndjson-test-patient");
    patient.addName().setFamily("NdjsonTestFamily");
    final String patientJson = fhirContext.newJsonParser().encodeResourceToString(patient);

    final Parameters parameters = createParametersWithInlineResources(viewJson,
        "application/x-ndjson", List.of(patientJson));

    final EntityExchangeResult<byte[]> result = webTestClient.post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/x-ndjson")
        .bodyValue(fhirContext.newJsonParser().encodeResourceToString(parameters))
        .exchange()
        .expectStatus().isOk()
        .expectHeader().contentTypeCompatibleWith(MediaType.parseMediaType("application/x-ndjson"))
        .expectBody()
        .returnResult();

    final String responseBody = new String(result.getResponseBodyContent(), StandardCharsets.UTF_8);
    log.debug("NDJSON response:\n{}", responseBody);

    assertThat(responseBody).isNotEmpty();
    assertThat(responseBody).contains("ndjson-test-patient");
    assertThat(responseBody).contains("NdjsonTestFamily");

    // Verify response is valid JSON.
    final String line = responseBody.trim();
    assertThat(line).startsWith("{");
    assertThat(line).endsWith("}");
    assertThat(line).contains("\"id\":");
  }

  @Test
  void csvOutputWithInlineResources() {
    final String viewJson = createSimplePatientView();
    final Patient patient = new Patient();
    patient.setId("csv-test-patient");
    patient.addName().setFamily("CsvTestFamily");
    final String patientJson = fhirContext.newJsonParser().encodeResourceToString(patient);

    final Parameters parameters = createParametersWithInlineResources(viewJson,
        "text/csv", List.of(patientJson));
    parameters.addParameter().setName("header")
        .setValue(new org.hl7.fhir.r4.model.BooleanType(true));

    final EntityExchangeResult<byte[]> result = webTestClient.post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "text/csv")
        .bodyValue(fhirContext.newJsonParser().encodeResourceToString(parameters))
        .exchange()
        .expectStatus().isOk()
        .expectHeader().contentTypeCompatibleWith(MediaType.parseMediaType("text/csv"))
        .expectBody()
        .returnResult();

    final String responseBody = new String(result.getResponseBodyContent(), StandardCharsets.UTF_8);
    log.debug("CSV response:\n{}", responseBody);

    assertThat(responseBody).isNotEmpty();
    final String[] lines = responseBody.trim().split("\n");
    assertThat(lines.length).isGreaterThanOrEqualTo(2);

    // First line should be header.
    assertThat(lines[0]).contains("id");
    assertThat(lines[0]).contains("family_name");
    // Second line should be data.
    assertThat(lines[1]).contains("csv-test-patient");
    assertThat(lines[1]).contains("CsvTestFamily");
  }

  @Test
  void multipleInlineResourcesEndToEnd() {
    final String viewJson = createSimplePatientView();
    final Patient patient1 = new Patient();
    patient1.setId("multi-patient-1");
    patient1.addName().setFamily("Family1");
    final Patient patient2 = new Patient();
    patient2.setId("multi-patient-2");
    patient2.addName().setFamily("Family2");
    final String patient1Json = fhirContext.newJsonParser().encodeResourceToString(patient1);
    final String patient2Json = fhirContext.newJsonParser().encodeResourceToString(patient2);

    final Parameters parameters = createParametersWithInlineResources(viewJson,
        "application/x-ndjson", List.of(patient1Json, patient2Json));

    final EntityExchangeResult<byte[]> result = webTestClient.post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/x-ndjson")
        .bodyValue(fhirContext.newJsonParser().encodeResourceToString(parameters))
        .exchange()
        .expectStatus().isOk()
        .expectHeader().contentTypeCompatibleWith(MediaType.parseMediaType("application/x-ndjson"))
        .expectBody()
        .returnResult();

    final String responseBody = new String(result.getResponseBodyContent(), StandardCharsets.UTF_8);
    log.debug("Multiple inline resources response:\n{}", responseBody);

    assertThat(responseBody).isNotEmpty();
    assertThat(responseBody).contains("multi-patient-1");
    assertThat(responseBody).contains("multi-patient-2");

    // Should have two lines since we provided two patients.
    final String[] lines = responseBody.trim().split("\n");
    assertThat(lines.length).isEqualTo(2);
  }

  @Test
  void invalidViewDefinitionReturns4xxError() {
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("viewResource").setValue(new StringType("{ invalid }"));

    webTestClient.post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-run")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/x-ndjson")
        .bodyValue(fhirContext.newJsonParser().encodeResourceToString(parameters))
        .exchange()
        .expectStatus().is4xxClientError();
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private String createSimplePatientView() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "test_patient_view");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put("select", List.of(
        Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
        Map.of("column", List.of(Map.of("name", "family_name", "path", "name.first().family")))
    ));
    return gson.toJson(view);
  }

  @Nonnull
  private Parameters createParameters(@Nonnull final String viewJson,
      @Nonnull final String format, final Boolean includeHeader) {
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("viewResource").setValue(new StringType(viewJson));
    parameters.addParameter().setName("_format").setValue(new StringType(format));
    if (includeHeader != null) {
      parameters.addParameter().setName("header")
          .setValue(new org.hl7.fhir.r4.model.BooleanType(includeHeader));
    }
    return parameters;
  }

  @Nonnull
  private Parameters createParametersWithInlineResources(@Nonnull final String viewJson,
      @Nonnull final String format, @Nonnull final List<String> inlineResources) {
    final Parameters parameters = new Parameters();
    parameters.addParameter().setName("viewResource").setValue(new StringType(viewJson));
    parameters.addParameter().setName("_format").setValue(new StringType(format));
    for (final String resource : inlineResources) {
      parameters.addParameter().setName("resource").setValue(new StringType(resource));
    }
    return parameters;
  }

}
