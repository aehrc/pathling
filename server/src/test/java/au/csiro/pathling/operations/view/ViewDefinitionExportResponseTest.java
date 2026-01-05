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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ViewDefinitionExportResponse}.
 *
 * @author John Grimes
 */
class ViewDefinitionExportResponseTest {

  // -------------------------------------------------------------------------
  // Manifest structure tests
  // -------------------------------------------------------------------------

  @Test
  void manifestContainsRequiredFields() {
    // The manifest should contain transactionTime, request, requiresAccessToken, output, and
    // error.
    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(),
            false);

    final Parameters parameters = response.toOutput();

    assertThat(hasParameter(parameters, "transactionTime")).isTrue();
    assertThat(hasParameter(parameters, "request")).isTrue();
    assertThat(hasParameter(parameters, "requiresAccessToken")).isTrue();
    // Output and error may be absent when empty.
  }

  @Test
  void manifestContainsKickOffRequest() {
    // The request parameter should contain the kick-off URL.
    final String kickOffUrl = "http://example.org/fhir/$viewdefinition-export?_format=csv";
    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(kickOffUrl, "http://example.org/fhir", List.of(), false);

    final Parameters parameters = response.toOutput();
    final String request = getStringParameter(parameters, "request");

    assertThat(request).isEqualTo(kickOffUrl);
  }

  @Test
  void manifestShowsRequiresAccessTokenFalse() {
    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(),
            false);

    final Parameters parameters = response.toOutput();
    final Boolean requiresAccessToken = getBooleanParameter(parameters, "requiresAccessToken");

    assertThat(requiresAccessToken).isFalse();
  }

  @Test
  void manifestShowsRequiresAccessTokenTrue() {
    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(),
            true);

    final Parameters parameters = response.toOutput();
    final Boolean requiresAccessToken = getBooleanParameter(parameters, "requiresAccessToken");

    assertThat(requiresAccessToken).isTrue();
  }

  @Test
  void manifestContainsTransactionTime() {
    // The transactionTime should be an instant value.
    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(),
            false);

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
    // Output entries should have name and url parts.
    final ViewExportOutput output =
        new ViewExportOutput(
            "patients", List.of("file:///tmp/jobs/abc-123/patients.ndjson/part-00000.json"));

    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(output),
            false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");

    assertThat(outputs).hasSize(1);

    final ParametersParameterComponent outputParam = outputs.get(0);
    assertThat(getPartValue(outputParam, "name")).isEqualTo("patients");
    // The URL should be transformed to a result URL.
    final String url = getPartValue(outputParam, "url");
    assertThat(url).contains("$result").contains("job=abc-123");
  }

  @Test
  void manifestContainsMultipleFilesPerOutput() {
    // When an output has multiple files, each gets its own output parameter.
    final ViewExportOutput output =
        new ViewExportOutput(
            "observations",
            List.of(
                "file:///tmp/jobs/job-id/observations.ndjson/part-00000.json",
                "file:///tmp/jobs/job-id/observations.ndjson/part-00001.json"));

    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(output),
            false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");

    assertThat(outputs).hasSize(2);
    assertThat(getPartValue(outputs.get(0), "name")).isEqualTo("observations");
    assertThat(getPartValue(outputs.get(1), "name")).isEqualTo("observations");
  }

  @Test
  void manifestContainsMultipleOutputs() {
    // Multiple ViewExportOutputs each produce their own output parameters.
    final ViewExportOutput output1 =
        new ViewExportOutput(
            "patients", List.of("file:///tmp/jobs/job-id/patients.csv/part-00000.csv"));
    final ViewExportOutput output2 =
        new ViewExportOutput(
            "observations", List.of("file:///tmp/jobs/job-id/observations.csv/part-00000.csv"));

    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(output1, output2),
            false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");

    assertThat(outputs).hasSize(2);
    assertThat(getPartValue(outputs.get(0), "name")).isEqualTo("patients");
    assertThat(getPartValue(outputs.get(1), "name")).isEqualTo("observations");
  }

  @Test
  void noOutputParametersWhenNoOutputs() {
    // When there are no outputs, no output parameters should be present.
    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir",
            List.of(),
            false);

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
    final ViewExportOutput output =
        new ViewExportOutput(
            "test", List.of("file:///tmp/jobs/job-id/test.ndjson/part-00000.json"));

    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir", // No trailing slash.
            List.of(output),
            false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");
    final String url = getPartValue(outputs.get(0), "url");

    // Should still produce a valid URL.
    assertThat(url).startsWith("http://example.org/fhir/$result");
  }

  @Test
  void serverBaseUrlWithTrailingSlashHandledCorrectly() {
    // Base URL with trailing slash should not produce double slashes.
    final ViewExportOutput output =
        new ViewExportOutput(
            "test", List.of("file:///tmp/jobs/job-id/test.ndjson/part-00000.json"));

    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            "http://example.org/fhir/$viewdefinition-export",
            "http://example.org/fhir/", // With trailing slash.
            List.of(output),
            false);

    final Parameters parameters = response.toOutput();
    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");
    final String url = getPartValue(outputs.get(0), "url");

    // Should not have double slash.
    assertThat(url).startsWith("http://example.org/fhir/$result");
    assertThat(url).doesNotContain("fhir//$result");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private static boolean hasParameter(final Parameters parameters, final String name) {
    return parameters.getParameter().stream().anyMatch(p -> name.equals(p.getName()));
  }

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

  private static String getStringParameter(final Parameters parameters, final String name) {
    final ParametersParameterComponent param = findParameter(parameters, name);
    if (param == null || !param.hasValue()) {
      return null;
    }
    if (param.getValue() instanceof final UriType uriType) {
      return uriType.getValue();
    }
    if (param.getValue() instanceof final StringType stringType) {
      return stringType.getValue();
    }
    return param.getValue().primitiveValue();
  }

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
              if (p.getValue() instanceof final StringType stringType) {
                return stringType.getValue();
              }
              return p.getValue().primitiveValue();
            })
        .orElse(null);
  }
}
