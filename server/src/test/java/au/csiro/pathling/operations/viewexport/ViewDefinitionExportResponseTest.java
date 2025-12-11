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

package au.csiro.pathling.operations.viewexport;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.hl7.fhir.r4.model.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ViewDefinitionExportResponse}.
 *
 * @author John Grimes
 */
class ViewDefinitionExportResponseTest {

  private ObjectMapper mapper;

  @BeforeEach
  void setUp() {
    mapper = new ObjectMapper();
  }

  // -------------------------------------------------------------------------
  // Manifest structure tests
  // -------------------------------------------------------------------------

  @Test
  void manifestContainsRequiredFields() throws Exception {
    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);

    assertThat(manifest.has("transactionTime")).isTrue();
    assertThat(manifest.has("request")).isTrue();
    assertThat(manifest.has("requiresAccessToken")).isTrue();
    assertThat(manifest.has("output")).isTrue();
    assertThat(manifest.has("error")).isTrue();
  }

  @Test
  void manifestHasCorrectContentType() {
    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(),
        false
    );

    final Binary binary = response.toOutput();

    assertThat(binary.getContentType()).isEqualTo("application/json");
  }

  @Test
  void manifestContainsKickOffRequest() throws Exception {
    final String kickOffUrl = "http://example.org/fhir/$viewdefinition-export?_format=csv";
    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        kickOffUrl,
        "http://example.org/fhir",
        List.of(),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);

    assertThat(manifest.get("request").asText()).isEqualTo(kickOffUrl);
  }

  @Test
  void manifestShowsRequiresAccessTokenFalse() throws Exception {
    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);

    assertThat(manifest.get("requiresAccessToken").asBoolean()).isFalse();
  }

  @Test
  void manifestShowsRequiresAccessTokenTrue() throws Exception {
    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(),
        true
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);

    assertThat(manifest.get("requiresAccessToken").asBoolean()).isTrue();
  }

  // -------------------------------------------------------------------------
  // Output entries tests
  // -------------------------------------------------------------------------

  @Test
  void manifestContainsOutputEntries() throws Exception {
    final ViewExportOutput output = new ViewExportOutput("patients",
        List.of("file:///tmp/jobs/abc-123/patients.ndjson/part-00000.json"));

    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(output),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);
    final JsonNode outputArray = manifest.get("output");

    assertThat(outputArray.isArray()).isTrue();
    assertThat(outputArray.size()).isEqualTo(1);
    assertThat(outputArray.get(0).get("name").asText()).isEqualTo("patients");
    // The URL should be transformed to a result URL.
    assertThat(outputArray.get(0).get("url").asText())
        .contains("$result")
        .contains("job=abc-123");
  }

  @Test
  void manifestContainsMultipleFilesPerOutput() throws Exception {
    final ViewExportOutput output = new ViewExportOutput("observations",
        List.of(
            "file:///tmp/jobs/job-id/observations.ndjson/part-00000.json",
            "file:///tmp/jobs/job-id/observations.ndjson/part-00001.json"
        ));

    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(output),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);
    final JsonNode outputArray = manifest.get("output");

    assertThat(outputArray.size()).isEqualTo(2);
    assertThat(outputArray.get(0).get("name").asText()).isEqualTo("observations");
    assertThat(outputArray.get(1).get("name").asText()).isEqualTo("observations");
  }

  @Test
  void manifestContainsMultipleOutputs() throws Exception {
    final ViewExportOutput output1 = new ViewExportOutput("patients",
        List.of("file:///tmp/jobs/job-id/patients.csv/part-00000.csv"));
    final ViewExportOutput output2 = new ViewExportOutput("observations",
        List.of("file:///tmp/jobs/job-id/observations.csv/part-00000.csv"));

    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(output1, output2),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);
    final JsonNode outputArray = manifest.get("output");

    assertThat(outputArray.size()).isEqualTo(2);
    assertThat(outputArray.get(0).get("name").asText()).isEqualTo("patients");
    assertThat(outputArray.get(1).get("name").asText()).isEqualTo("observations");
  }

  @Test
  void emptyOutputArrayWhenNoOutputs() throws Exception {
    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);
    final JsonNode outputArray = manifest.get("output");

    assertThat(outputArray.isArray()).isTrue();
    assertThat(outputArray.size()).isZero();
  }

  @Test
  void errorArrayIsAlwaysEmpty() throws Exception {
    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",
        List.of(),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);
    final JsonNode errorArray = manifest.get("error");

    assertThat(errorArray.isArray()).isTrue();
    assertThat(errorArray.size()).isZero();
  }

  // -------------------------------------------------------------------------
  // URL normalisation tests
  // -------------------------------------------------------------------------

  @Test
  void serverBaseUrlNormalisedWithTrailingSlash() throws Exception {
    // Base URL without trailing slash.
    final ViewExportOutput output = new ViewExportOutput("test",
        List.of("file:///tmp/jobs/job-id/test.ndjson/part-00000.json"));

    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir",  // No trailing slash.
        List.of(output),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);
    final String url = manifest.get("output").get(0).get("url").asText();

    // Should still produce a valid URL.
    assertThat(url).startsWith("http://example.org/fhir/$result");
  }

  @Test
  void serverBaseUrlWithTrailingSlashHandledCorrectly() throws Exception {
    // Base URL with trailing slash.
    final ViewExportOutput output = new ViewExportOutput("test",
        List.of("file:///tmp/jobs/job-id/test.ndjson/part-00000.json"));

    final ViewDefinitionExportResponse response = new ViewDefinitionExportResponse(
        "http://example.org/fhir/$viewdefinition-export",
        "http://example.org/fhir/",  // With trailing slash.
        List.of(output),
        false
    );

    final Binary binary = response.toOutput();
    final JsonNode manifest = parseManifest(binary);
    final String url = manifest.get("output").get(0).get("url").asText();

    // Should not have double slash.
    assertThat(url).startsWith("http://example.org/fhir/$result");
    assertThat(url).doesNotContain("fhir//$result");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private JsonNode parseManifest(final Binary binary) throws Exception {
    final String json = new String(binary.getContent(), StandardCharsets.UTF_8);
    return mapper.readTree(json);
  }

}
