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

import java.time.Instant;
import java.util.List;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ViewDefinitionExportResponse}, verifying the SQL on FHIR completion
 * manifest shape (US9).
 *
 * @author John Grimes
 */
class ViewDefinitionExportResponseTest {

  private static final String BASE_URL = "http://example.org/fhir";
  private static final Instant START = Instant.parse("2026-06-21T01:00:00Z");
  private static final Instant END = Instant.parse("2026-06-21T01:00:12Z");

  // -------------------------------------------------------------------------
  // Required manifest fields
  // -------------------------------------------------------------------------

  @Test
  void manifestContainsExportIdAndCompletedStatus() {
    final Parameters parameters = response("job-123", "tracking-1", "ndjson", List.of()).toOutput();

    assertThat(getStringParameter(parameters, "exportId")).isEqualTo("job-123");
    assertThat(getStringParameter(parameters, "status")).isEqualTo("completed");
  }

  @Test
  void manifestEchoesEffectiveFormat() {
    final Parameters parameters = response("job-123", null, "csv", List.of()).toOutput();
    assertThat(getStringParameter(parameters, "_format")).isEqualTo("csv");
  }

  @Test
  void manifestEchoesClientTrackingIdWhenSupplied() {
    final Parameters parameters =
        response("job-123", "tracking-abc", "ndjson", List.of()).toOutput();
    assertThat(getStringParameter(parameters, "clientTrackingId")).isEqualTo("tracking-abc");
  }

  @Test
  void manifestOmitsClientTrackingIdWhenAbsent() {
    final Parameters parameters = response("job-123", null, "ndjson", List.of()).toOutput();
    assertThat(hasParameter(parameters, "clientTrackingId")).isFalse();
  }

  @Test
  void manifestContainsTimingFields() {
    final Parameters parameters = response("job-123", null, "ndjson", List.of()).toOutput();

    assertThat(findParameter(parameters, "exportStartTime").getValue())
        .isInstanceOf(InstantType.class);
    assertThat(findParameter(parameters, "exportEndTime").getValue())
        .isInstanceOf(InstantType.class);

    final ParametersParameterComponent duration = findParameter(parameters, "exportDuration");
    assertThat(duration.getValue()).isInstanceOf(IntegerType.class);
    assertThat(((IntegerType) duration.getValue()).getValue()).isEqualTo(12);
  }

  @Test
  void manifestDoesNotContainNonSpecFields() {
    final Parameters parameters = response("job-123", "t", "ndjson", List.of()).toOutput();

    assertThat(hasParameter(parameters, "transactionTime")).isFalse();
    assertThat(hasParameter(parameters, "request")).isFalse();
    assertThat(hasParameter(parameters, "requiresAccessToken")).isFalse();
  }

  // -------------------------------------------------------------------------
  // Output entries
  // -------------------------------------------------------------------------

  @Test
  void outputHasNameAndLocationPartsAndNoUrlPart() {
    final ViewExportOutput output =
        new ViewExportOutput(
            "patients", List.of("file:///tmp/jobs/abc-123/patients.ndjson/part-00000.json"));
    final Parameters parameters = response("abc-123", null, "ndjson", List.of(output)).toOutput();

    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");
    assertThat(outputs).hasSize(1);

    final ParametersParameterComponent outputParam = outputs.get(0);
    assertThat(getPartValue(outputParam, "name")).isEqualTo("patients");
    assertThat(getPartValue(outputParam, "location")).contains("$result").contains("job=abc-123");
    assertThat(hasPart(outputParam, "url")).isFalse();
  }

  @Test
  void viewWithMultipleFilesYieldsOneOutputWithRepeatingLocations() {
    final ViewExportOutput output =
        new ViewExportOutput(
            "observations",
            List.of(
                "file:///tmp/jobs/job-id/observations.ndjson/part-00000.json",
                "file:///tmp/jobs/job-id/observations.ndjson/part-00001.json"));
    final Parameters parameters = response("job-id", null, "ndjson", List.of(output)).toOutput();

    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");
    // One output per view, regardless of the number of files.
    assertThat(outputs).hasSize(1);
    final List<ParametersParameterComponent> locations =
        outputs.get(0).getPart().stream().filter(p -> "location".equals(p.getName())).toList();
    assertThat(locations).hasSize(2);
  }

  @Test
  void multipleViewsYieldOneOutputEach() {
    final ViewExportOutput patients =
        new ViewExportOutput(
            "patients", List.of("file:///tmp/jobs/job-id/patients.csv/part-00000.csv"));
    final ViewExportOutput observations =
        new ViewExportOutput(
            "observations", List.of("file:///tmp/jobs/job-id/observations.csv/part-00000.csv"));
    final Parameters parameters =
        response("job-id", null, "csv", List.of(patients, observations)).toOutput();

    final List<ParametersParameterComponent> outputs = getParametersByName(parameters, "output");
    assertThat(outputs).hasSize(2);
    assertThat(getPartValue(outputs.get(0), "name")).isEqualTo("patients");
    assertThat(getPartValue(outputs.get(1), "name")).isEqualTo("observations");
  }

  @Test
  void noOutputParametersWhenNoOutputs() {
    final Parameters parameters = response("job-id", null, "ndjson", List.of()).toOutput();
    assertThat(getParametersByName(parameters, "output")).isEmpty();
  }

  @Test
  void serverBaseUrlNormalisationDoesNotProduceDoubleSlash() {
    final ViewExportOutput output =
        new ViewExportOutput(
            "test", List.of("file:///tmp/jobs/job-id/test.ndjson/part-00000.json"));
    final ViewDefinitionExportResponse response =
        new ViewDefinitionExportResponse(
            BASE_URL + "/", List.of(output), "job-id", null, "ndjson", START, END);

    final List<ParametersParameterComponent> outputs =
        getParametersByName(response.toOutput(), "output");
    final String location = getPartValue(outputs.get(0), "location");
    assertThat(location).startsWith("http://example.org/fhir/$result");
    assertThat(location).doesNotContain("fhir//$result");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private static ViewDefinitionExportResponse response(
      final String exportId,
      final String clientTrackingId,
      final String format,
      final List<ViewExportOutput> outputs) {
    return new ViewDefinitionExportResponse(
        BASE_URL, outputs, exportId, clientTrackingId, format, START, END);
  }

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
    return param.getValue().primitiveValue();
  }

  private static boolean hasPart(final ParametersParameterComponent param, final String partName) {
    return param.getPart().stream().anyMatch(p -> partName.equals(p.getName()));
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
