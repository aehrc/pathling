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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import java.util.List;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UriType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExportResponse}.
 *
 * @author John Grimes
 */
class ExportResponseTest {

  // -------------------------------------------------------------------------
  // Manifest structure tests
  // -------------------------------------------------------------------------

  @Test
  void manifestContainsRequiredFields() {
    // The manifest should contain transactionTime, request, requiresAccessToken.
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();

    assertThat(hasParameter(parameters, "transactionTime")).isTrue();
    assertThat(hasParameter(parameters, "request")).isTrue();
    assertThat(hasParameter(parameters, "requiresAccessToken")).isTrue();
  }

  @Test
  void manifestContainsKickOffRequest() {
    // The request parameter should contain the kick-off URL.
    final String kickOffUrl = "http://example.org/fhir/$export?_type=Patient";
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(kickOffUrl, "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final String request = getStringParameter(parameters, "request");

    assertThat(request).isEqualTo(kickOffUrl);
  }

  @Test
  void manifestShowsRequiresAccessTokenFalse() {
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final Boolean requiresAccessToken = getBooleanParameter(parameters, "requiresAccessToken");

    assertThat(requiresAccessToken).isFalse();
  }

  @Test
  void manifestShowsRequiresAccessTokenTrue() {
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, true);

    final Parameters parameters = response.toOutput();
    final Boolean requiresAccessToken = getBooleanParameter(parameters, "requiresAccessToken");

    assertThat(requiresAccessToken).isTrue();
  }

  @Test
  void manifestContainsTransactionTime() {
    // The transactionTime should be an instant value.
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final ParametersParameterComponent param = findParameter(parameters, "transactionTime");

    assertThat(param).isNotNull();
    assertThat(param.getValue()).isInstanceOf(InstantType.class);
  }

  // -------------------------------------------------------------------------
  // Output entries tests
  // -------------------------------------------------------------------------

  @Test
  void manifestContainsOutputEntries() {
    // Output entries should have type and url parts.
    final FileInformation fileInfo =
        new FileInformation("Patient", "file:///tmp/jobs/abc-123/Patient.ndjson");
    final WriteDetails writeDetails = new WriteDetails(List.of(fileInfo));

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");

    assertThat(outputs).hasSize(1);

    final ParametersParameterComponent outputParam = outputs.getFirst();
    assertThat(getPartValue(outputParam, "type")).isEqualTo("Patient");
    // The URL should be transformed to a result URL.
    final String url = getPartValue(outputParam, "url");
    assertThat(url).contains("$result").contains("job=abc-123");
  }

  @Test
  void manifestContainsMultipleOutputEntries() {
    // Multiple file infos should each produce their own output parameters.
    final FileInformation patientFile =
        new FileInformation("Patient", "file:///tmp/jobs/job-id/Patient.ndjson");
    final FileInformation observationFile =
        new FileInformation("Observation", "file:///tmp/jobs/job-id/Observation.ndjson");
    final WriteDetails writeDetails = new WriteDetails(List.of(patientFile, observationFile));

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");

    assertThat(outputs).hasSize(2);
    assertThat(getPartValue(outputs.get(0), "type")).isEqualTo("Patient");
    assertThat(getPartValue(outputs.get(1), "type")).isEqualTo("Observation");
  }

  @Test
  void noOutputParametersWhenNoFiles() {
    // When there are no files, no output parameters should be present.
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");

    assertThat(outputs).isEmpty();
  }

  // -------------------------------------------------------------------------
  // URL normalisation tests
  // -------------------------------------------------------------------------

  @Test
  void serverBaseUrlNormalisedWithTrailingSlash() {
    // Base URL without trailing slash should still produce valid URLs.
    final FileInformation fileInfo =
        new FileInformation("Patient", "file:///tmp/jobs/job-id/Patient.ndjson");
    final WriteDetails writeDetails = new WriteDetails(List.of(fileInfo));

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export",
            "http://example.org/fhir", // No trailing slash.
            writeDetails,
            false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");
    final String url = getPartValue(outputs.getFirst(), "url");

    // Should still produce a valid URL.
    assertThat(url).startsWith("http://example.org/fhir/$result");
  }

  @Test
  void serverBaseUrlWithTrailingSlashHandledCorrectly() {
    // Base URL with trailing slash should not produce double slashes.
    final FileInformation fileInfo =
        new FileInformation("Patient", "file:///tmp/jobs/job-id/Patient.ndjson");
    final WriteDetails writeDetails = new WriteDetails(List.of(fileInfo));

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export",
            "http://example.org/fhir/", // With trailing slash.
            writeDetails,
            false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");
    final String url = getPartValue(outputs.getFirst(), "url");

    // Should not have double slash.
    assertThat(url).startsWith("http://example.org/fhir/$result").doesNotContain("fhir//$result");
  }

  // -------------------------------------------------------------------------
  // Native JSON tests
  // -------------------------------------------------------------------------

  @Test
  void manifestIncludesEmptyErrorArray() {
    // The manifest should always include an error parameter, even when there are no errors.
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();

    assertThat(hasParameter(parameters, "error")).isTrue();
  }

  @Test
  void manifestAttachesNativeJson() {
    // The Parameters should have native JSON attached via userData.
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final Object nativeJson = parameters.getUserData("nativeJson");

    assertThat(nativeJson).isNotNull();
    assertThat(nativeJson).isInstanceOf(String.class);
  }

  @Test
  void nativeJsonHasCorrectStructure() throws Exception {
    // The native JSON should have output and error as arrays, with all expected fields.
    final FileInformation patientFile =
        new FileInformation("Patient", "file:///tmp/jobs/job-id/Patient.ndjson");
    final FileInformation observationFile =
        new FileInformation("Observation", "file:///tmp/jobs/job-id/Observation.ndjson");
    final WriteDetails writeDetails = new WriteDetails(List.of(patientFile, observationFile));

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final String nativeJson = (String) parameters.getUserData("nativeJson");
    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode json = mapper.readTree(nativeJson);

    // Verify all expected fields are present.
    assertThat(json.has("transactionTime")).isTrue();
    assertThat(json.has("request")).isTrue();
    assertThat(json.has("requiresAccessToken")).isTrue();
    assertThat(json.has("output")).isTrue();
    assertThat(json.has("error")).isTrue();

    // Verify output is always an array.
    assertThat(json.get("output").isArray()).isTrue();
    assertThat(json.get("output").size()).isEqualTo(2);
    assertThat(json.get("output").get(0).get("type").asText()).isEqualTo("Patient");
    assertThat(json.get("output").get(1).get("type").asText()).isEqualTo("Observation");

    // Verify error is always an array (empty when no errors).
    assertThat(json.get("error").isArray()).isTrue();
    assertThat(json.get("error").isEmpty()).isTrue();

    // Verify field values.
    assertThat(json.get("request").asText()).isEqualTo("http://example.org/fhir/$export");
    assertThat(json.get("requiresAccessToken").asBoolean()).isFalse();
  }

  @Test
  void nativeJsonOutputIsEmptyArrayWhenNoFiles() throws Exception {
    // When there are no files, output should be an empty array, not missing.
    final WriteDetails writeDetails = new WriteDetails(List.of());

    final ExportResponse response =
        new ExportResponse(
            "http://example.org/fhir/$export", "http://example.org/fhir", writeDetails, false);

    final Parameters parameters = response.toOutput();
    final String nativeJson = (String) parameters.getUserData("nativeJson");
    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode json = mapper.readTree(nativeJson);

    assertThat(json.get("output").isArray()).isTrue();
    assertThat(json.get("output").isEmpty()).isTrue();
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private static boolean hasParameter(final Parameters parameters, final String name) {
    return parameters.getParameter().stream().anyMatch(p -> name.equals(p.getName()));
  }

  @Nullable
  private static ParametersParameterComponent findParameter(
      final Parameters parameters, final String name) {
    return parameters.getParameter().stream()
        .filter(p -> name.equals(p.getName()))
        .findFirst()
        .orElse(null);
  }

  private static List<ParametersParameterComponent> getParametersByName(
      final Parameters parameters, final String name) {
    return parameters.getParameter().stream().filter(p -> name.equals(p.getName())).toList();
  }

  @Nullable
  private static String getStringParameter(final Parameters parameters, final String name) {
    final ParametersParameterComponent param = findParameter(parameters, name);
    if (param == null || !param.hasValue()) {
      return null;
    }
    if (param.getValue() instanceof final UriType uriType) {
      return uriType.getValue();
    }
    return param.getValue().primitiveValue();
  }

  @Nullable
  private static Boolean getBooleanParameter(final Parameters parameters, final String name) {
    final ParametersParameterComponent param = findParameter(parameters, name);
    if (param == null || !param.hasValue()) {
      return null;
    }
    if (param.getValue() instanceof final BooleanType booleanType) {
      return booleanType.getValue();
    }
    return null;
  }

  @Nullable
  private static String getPartValue(
      final ParametersParameterComponent param, final String partName) {
    return param.getPart().stream()
        .filter(p -> partName.equals(p.getName()))
        .findFirst()
        .map(
            p -> {
              if (p.getValue() instanceof final UriType uriType) {
                return uriType.getValue();
              }
              if (p.getValue() instanceof final CodeType codeType) {
                return codeType.getValue();
              }
              return p.getValue().primitiveValue();
            })
        .orElse(null);
  }
}
