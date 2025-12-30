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

import au.csiro.pathling.encoders.ViewDefinitionResource;
import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration tests for {@link ViewDefinitionExportProvider}.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"integration-test"})
class ViewDefinitionExportProviderIT {

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  private static Path warehouseDir;

  @Autowired private FhirContext fhirContext;

  private Gson gson;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    warehouseDir = Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
  }

  @BeforeEach
  void setup() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .build();
    gson = new GsonBuilder().create();
    // Register ViewDefinitionResource with the FhirContext so it can be serialised/parsed.
    fhirContext.registerCustomType(ViewDefinitionResource.class);
  }

  /**
   * Tests that the $viewdefinition-export operation accepts the nested Parameters structure as
   * documented. This test reproduces the structure sent by the UI.
   */
  @Test
  void kickOffWithNestedViewResourceParameter() {
    final String parametersJson = createExportParametersWithNestedView();
    log.debug("Request body:\n{}", parametersJson);

    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(parametersJson)
        .exchange()
        .expectStatus()
        .isAccepted()
        .expectHeader()
        .exists("Content-Location");
  }

  /** Tests that the $viewdefinition-export operation accepts multiple views with names. */
  @Test
  void kickOffWithMultipleNamedViews() {
    final String parametersJson = createExportParametersWithMultipleViews();
    log.debug("Request body:\n{}", parametersJson);

    webTestClient
        .post()
        .uri("http://localhost:" + port + "/fhir/$viewdefinition-export")
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(parametersJson)
        .exchange()
        .expectStatus()
        .isAccepted()
        .expectHeader()
        .exists("Content-Location");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  /**
   * Creates a Parameters JSON with nested view structure matching what the UI sends.
   *
   * <p>Structure:
   *
   * <pre>
   * {
   *   "resourceType": "Parameters",
   *   "parameter": [
   *     {
   *       "name": "view",
   *       "part": [
   *         { "name": "viewResource", "resource": { ... ViewDefinition ... } }
   *       ]
   *     },
   *     { "name": "_format", "valueString": "ndjson" }
   *   ]
   * }
   * </pre>
   */
  @Nonnull
  private String createExportParametersWithNestedView() {
    final Map<String, Object> viewDefinition = createSimplePatientViewDefinition();

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    // Build the nested view parameter.
    final Map<String, Object> viewParam = new LinkedHashMap<>();
    viewParam.put("name", "view");
    final List<Map<String, Object>> parts = new ArrayList<>();
    final Map<String, Object> viewResourcePart = new LinkedHashMap<>();
    viewResourcePart.put("name", "viewResource");
    viewResourcePart.put("resource", viewDefinition);
    parts.add(viewResourcePart);
    viewParam.put("part", parts);
    parameterList.add(viewParam);

    // Add format parameter.
    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", "ndjson");
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return gson.toJson(parameters);
  }

  /** Creates a Parameters JSON with multiple named views. */
  @Nonnull
  private String createExportParametersWithMultipleViews() {
    final Map<String, Object> patientView = createSimplePatientViewDefinition();
    patientView.put("name", "patient_demographics");

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    final List<Map<String, Object>> parameterList = new ArrayList<>();

    // First view with name.
    final Map<String, Object> viewParam1 = new LinkedHashMap<>();
    viewParam1.put("name", "view");
    final List<Map<String, Object>> parts1 = new ArrayList<>();

    final Map<String, Object> namePart1 = new LinkedHashMap<>();
    namePart1.put("name", "name");
    namePart1.put("valueString", "patients");
    parts1.add(namePart1);

    final Map<String, Object> viewResourcePart1 = new LinkedHashMap<>();
    viewResourcePart1.put("name", "viewResource");
    viewResourcePart1.put("resource", patientView);
    parts1.add(viewResourcePart1);

    viewParam1.put("part", parts1);
    parameterList.add(viewParam1);

    // Add format parameter.
    final Map<String, Object> formatParam = new LinkedHashMap<>();
    formatParam.put("name", "_format");
    formatParam.put("valueString", "ndjson");
    parameterList.add(formatParam);

    parameters.put("parameter", parameterList);
    return gson.toJson(parameters);
  }

  /** Creates a simple ViewDefinition for Patient resources. */
  @Nonnull
  private Map<String, Object> createSimplePatientViewDefinition() {
    final Map<String, Object> view = new HashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("name", "test_patient_view");
    view.put("resource", "Patient");
    view.put("status", "active");
    view.put(
        "select",
        List.of(
            Map.of("column", List.of(Map.of("name", "id", "path", "id"))),
            Map.of(
                "column", List.of(Map.of("name", "family_name", "path", "name.first().family")))));
    return view;
  }
}
