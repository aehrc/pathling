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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Emits a {@code benchmark-report.json} conforming to the contract-v2 {@code
 * benchmark-report.schema.json}.
 *
 * <p>The report records a structured {@code implementation} (engine plus language binding), the
 * {@code benchmark} and {@code dataset} identities, the {@code environment}, a {@code measurement}
 * block declaring the scenario, timed phases, sink and actual warmup and iteration counts, and one
 * {@code results} entry keyed by the authored suite name carrying the size, FHIR version, per-size
 * resource counts, and one entry per case keyed by its stable id with status, input and output row
 * counts, timing samples, the fixed {@code stats} set, and separately recorded load samples.
 */
public final class ReportWriter {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  private ReportWriter() {}

  /**
   * Builds and writes the benchmark report.
   *
   * @param out the path to write the report to
   * @param engineName the execution engine name (e.g. {@code Pathling})
   * @param engineVersion the execution engine version
   * @param bindingName the language binding name (e.g. {@code pathling-java})
   * @param bindingVersion the language binding version
   * @param benchmarkName the authored suite name (also the {@code results} map key)
   * @param benchmarkVersion the authored suite version
   * @param datasetName the dataset name identity
   * @param datasetVersion the dataset version identity
   * @param environment the environment description
   * @param scenario the measurement scenario (e.g. {@code preloaded_repeated})
   * @param sink the sink used for the timed region (e.g. {@code csv})
   * @param warmup the actual warmup iteration count
   * @param iterations the actual measured iteration count
   * @param size the size key the benchmark was run at
   * @param fhirVersion the FHIR version
   * @param resourceCounts the per-resource-type row counts observed at this size
   * @param results the per-case results
   */
  public static void write(
      @Nonnull final Path out,
      @Nonnull final String engineName,
      @Nonnull final String engineVersion,
      @Nonnull final String bindingName,
      @Nonnull final String bindingVersion,
      @Nonnull final String benchmarkName,
      @Nonnull final String benchmarkVersion,
      @Nonnull final String datasetName,
      @Nonnull final String datasetVersion,
      @Nonnull final ObjectNode environment,
      @Nonnull final String scenario,
      @Nonnull final String sink,
      final int warmup,
      final int iterations,
      @Nonnull final String size,
      @Nonnull final String fhirVersion,
      @Nonnull final Map<String, Integer> resourceCounts,
      @Nonnull final List<CaseResult> results) {
    final ObjectNode report = MAPPER.createObjectNode();

    final ObjectNode implementation = report.putObject("implementation");
    nameVersion(implementation.putObject("engine"), engineName, engineVersion);
    nameVersion(implementation.putObject("binding"), bindingName, bindingVersion);

    nameVersion(report.putObject("benchmark"), benchmarkName, benchmarkVersion);
    nameVersion(report.putObject("dataset"), datasetName, datasetVersion);
    report.set("environment", environment);

    final ObjectNode measurement = report.putObject("measurement");
    measurement.put("scenario", scenario);
    final ArrayNode phases = measurement.putArray("phases");
    phases.add("execute");
    phases.add("extract");
    measurement.put("sink", sink);
    measurement.put("warmup", warmup);
    measurement.put("iterations", iterations);

    final ObjectNode resultsNode = report.putObject("results");
    final ObjectNode benchmarkNode = resultsNode.putObject(benchmarkName);
    benchmarkNode.put("size", size);
    benchmarkNode.put("fhirVersion", fhirVersion);
    final ObjectNode resourceCountsNode = benchmarkNode.putObject("resourceCounts");
    resourceCounts.forEach(resourceCountsNode::put);
    final ArrayNode casesNode = benchmarkNode.putArray("cases");
    for (final CaseResult result : results) {
      casesNode.add(caseNode(result));
    }

    try {
      Files.write(out, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(report));
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to write benchmark report: " + out, e);
    }
  }

  private static void nameVersion(
      @Nonnull final ObjectNode node, @Nonnull final String name, @Nonnull final String version) {
    node.put("name", name);
    node.put("version", version);
  }

  @Nonnull
  private static ObjectNode caseNode(@Nonnull final CaseResult result) {
    final ObjectNode node = MAPPER.createObjectNode();
    node.put("id", result.getId());
    node.put("status", result.getStatus());
    // The advisory message accompanies a non-ok outcome only; an ok case omits it.
    result.getMessage().ifPresent(message -> node.put("message", message));

    final List<Double> samples = result.getExecuteExtractSamplesMs();
    // A failed case has no timing samples, so it carries neither measurements nor stats — only its
    // id, status and advisory message.
    if (samples.isEmpty()) {
      return node;
    }

    node.put("inputRows", result.getInputRows());
    node.put("outputRows", result.getOutputRows());

    final ArrayNode samplesNode = node.putArray("samplesMs");
    samples.forEach(samplesNode::add);

    node.set("stats", Statistics.toNode(MAPPER, samples));

    final ObjectNode phaseSamples = node.putObject("phaseSamplesMs");
    phaseSamples.putArray("load").add(result.getLoadMs());
    final ArrayNode executeExtract = phaseSamples.putArray("executeExtract");
    samples.forEach(executeExtract::add);

    return node;
  }
}
