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

package au.csiro.pathling.sof.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReportWriterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonNode writeAndRead(final Path out) throws Exception {
    final ObjectNode environment = MAPPER.createObjectNode();
    environment.put("os", "Test OS");
    final List<CaseResult> results =
        List.of(
            new CaseResult("condition-flat", "ok", 2L, 2L, List.of(10.0, 12.0, 11.0), 42.0),
            new CaseResult(
                "observation-components", "ok", 2L, 2L, List.of(20.0, 21.0, 19.0), 42.0));

    ReportWriter.write(
        out,
        "Pathling",
        "9.9.0",
        "pathling-java",
        "9.9.0",
        "clinical-flat",
        "1",
        "synthea-clinical",
        "1",
        environment,
        "preloaded_repeated",
        "csv",
        1,
        3,
        "s",
        "4.0.1",
        Map.of("Condition", 2, "Observation", 2),
        results);

    return MAPPER.readTree(Files.readAllBytes(out));
  }

  @Test
  void emitsStructuredImplementationAndProvenance(@TempDir final Path dir) throws Exception {
    final JsonNode report = writeAndRead(dir.resolve("report.json"));

    assertEquals("Pathling", report.at("/implementation/engine/name").asText());
    assertEquals("9.9.0", report.at("/implementation/engine/version").asText());
    assertEquals("pathling-java", report.at("/implementation/binding/name").asText());

    assertEquals("clinical-flat", report.at("/benchmark/name").asText());
    assertEquals("1", report.at("/benchmark/version").asText());
    assertEquals("synthea-clinical", report.at("/dataset/name").asText());
    assertEquals("1", report.at("/dataset/version").asText());

    // Removed pre-v2 fields must be absent.
    assertTrue(report.path("benchmarkVersion").isMissingNode());
    assertTrue(report.at("/implementation/name").isMissingNode());
  }

  @Test
  void emitsScenarioAndCsvSink(@TempDir final Path dir) throws Exception {
    final JsonNode measurement = writeAndRead(dir.resolve("report.json")).path("measurement");

    assertEquals("preloaded_repeated", measurement.path("scenario").asText());
    assertEquals("csv", measurement.path("sink").asText());
    assertEquals(1, measurement.path("warmup").asInt());
    assertEquals(3, measurement.path("iterations").asInt());
  }

  @Test
  void resultsKeyedBySuiteNameWithCasesById(@TempDir final Path dir) throws Exception {
    final JsonNode report = writeAndRead(dir.resolve("report.json"));
    final JsonNode entry = report.at("/results/clinical-flat");

    assertEquals("s", entry.path("size").asText());
    assertEquals("4.0.1", entry.path("fhirVersion").asText());
    assertEquals(2, entry.at("/resourceCounts/Condition").asInt());

    final JsonNode firstCase = entry.path("cases").get(0);
    assertEquals("condition-flat", firstCase.path("id").asText());
    assertTrue(firstCase.path("title").isMissingNode());
    assertEquals("ok", firstCase.path("status").asText());
    assertEquals(2, firstCase.path("inputRows").asInt());
    assertTrue(firstCase.path("samplesMs").isArray());
  }

  @Test
  void statsCarriesExactlyTheFixedKeySet(@TempDir final Path dir) throws Exception {
    final JsonNode stats =
        writeAndRead(dir.resolve("report.json")).at("/results/clinical-flat/cases/0/stats");

    final Set<String> keys =
        StreamSupport.stream(((Iterable<String>) stats::fieldNames).spliterator(), false)
            .collect(Collectors.toSet());
    assertEquals(Set.of("mean", "stddev", "min", "max", "median"), keys);
  }

  @Test
  void failedCaseRecordsExecutionErrorWithMessageAndNoSamples(@TempDir final Path dir)
      throws Exception {
    final ObjectNode environment = MAPPER.createObjectNode();
    environment.put("os", "Test OS");
    final List<CaseResult> results =
        List.of(
            new CaseResult("condition-flat", "ok", 2L, 2L, List.of(10.0, 12.0, 11.0), 42.0),
            CaseResult.failed("broken-case", "boom: could not evaluate view"));

    final Path out = dir.resolve("report.json");
    ReportWriter.write(
        out,
        "Pathling",
        "9.9.0",
        "pathling-java",
        "9.9.0",
        "clinical-flat",
        "1",
        "synthea-clinical",
        "1",
        environment,
        "preloaded_repeated",
        "csv",
        1,
        3,
        "s",
        "4.0.1",
        Map.of("Condition", 2, "Observation", 2),
        results);
    final JsonNode cases =
        MAPPER.readTree(Files.readAllBytes(out)).at("/results/clinical-flat/cases");

    // The succeeding case keeps its measurements and omits the advisory message.
    final JsonNode okCase = cases.get(0);
    assertEquals("ok", okCase.path("status").asText());
    assertTrue(okCase.path("message").isMissingNode());
    assertTrue(okCase.path("samplesMs").isArray());

    // The failed case is recorded as execution_error with a message and no measurements.
    final JsonNode failedCase = cases.get(1);
    assertEquals("broken-case", failedCase.path("id").asText());
    assertEquals("execution_error", failedCase.path("status").asText());
    assertEquals("boom: could not evaluate view", failedCase.path("message").asText());
    assertTrue(failedCase.path("samplesMs").isMissingNode());
    assertTrue(failedCase.path("stats").isMissingNode());
    assertTrue(failedCase.path("inputRows").isMissingNode());
  }

  @Test
  void satisfiesTopLevelSchemaRequiredKeys(@TempDir final Path dir) throws Exception {
    final JsonNode report = writeAndRead(dir.resolve("report.json"));
    final JsonNode schema =
        MAPPER.readTree(
            Files.readAllBytes(TestResources.path("/contract-v2/benchmark-report.schema.json")));

    final Iterator<JsonNode> required = schema.path("required").elements();
    while (required.hasNext()) {
      final String key = required.next().asText();
      assertFalse(
          report.path(key).isMissingNode(), () -> "report is missing schema-required key: " + key);
    }
  }
}
