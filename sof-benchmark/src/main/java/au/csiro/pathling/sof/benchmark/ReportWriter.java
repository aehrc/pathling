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
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * Emits a {@code benchmark-report.json} conforming to {@code benchmark-report.schema.json}.
 *
 * <p>The report records the implementation (name and version), the optional benchmark version and
 * environment, the {@code measurement} block declaring the timed phases, sink and actual warmup and
 * iteration counts, and one {@code results} entry per benchmark keyed by title with each case's
 * status, input and output row counts, execute+extract samples, summary statistics, and the
 * separately recorded load samples.
 */
public final class ReportWriter {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The sink categories permitted by {@code benchmark-report.schema.json}. */
  @Nonnull
  private static final Set<String> SCHEMA_SINKS = Set.of("table", "csv", "memory", "other");

  private ReportWriter() {}

  /**
   * Builds and writes the benchmark report.
   *
   * @param out the path to write the report to
   * @param implementationName the implementation name (e.g. {@code pathling-java})
   * @param version the implementation version
   * @param benchmarkVersion the benchmark submodule git SHA, or null if unavailable
   * @param environment the environment description (os, java, spark, cores)
   * @param sink the Spark write format used for the timed region
   * @param warmup the actual warmup iteration count
   * @param iterations the actual measured iteration count
   * @param benchmarkTitle the benchmark title (the {@code results} key)
   * @param size the size key the benchmark was run at
   * @param fhirVersion the FHIR version
   * @param results the per-case results
   */
  public static void write(
      @Nonnull final Path out,
      @Nonnull final String implementationName,
      @Nonnull final String version,
      @Nullable final String benchmarkVersion,
      @Nonnull final ObjectNode environment,
      @Nonnull final String sink,
      final int warmup,
      final int iterations,
      @Nonnull final String benchmarkTitle,
      @Nonnull final String size,
      @Nonnull final String fhirVersion,
      @Nonnull final List<CaseResult> results) {
    final ObjectNode report = MAPPER.createObjectNode();

    final ObjectNode implementation = report.putObject("implementation");
    implementation.put("name", implementationName);
    implementation.put("version", version);

    if (benchmarkVersion != null) {
      report.put("benchmarkVersion", benchmarkVersion);
    }
    report.set("environment", environment);

    final ObjectNode measurement = report.putObject("measurement");
    final ArrayNode phases = measurement.putArray("phases");
    phases.add("execute");
    phases.add("extract");
    measurement.put("sink", schemaSink(sink));
    measurement.put("warmup", warmup);
    measurement.put("iterations", iterations);

    final ObjectNode resultsNode = report.putObject("results");
    final ObjectNode benchmarkNode = resultsNode.putObject(benchmarkTitle);
    benchmarkNode.put("size", size);
    benchmarkNode.put("fhirVersion", fhirVersion);
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

  /**
   * Maps a Spark write format to a sink category permitted by the report schema. File formats such
   * as {@code parquet} that are not enumerated by the schema are reported as {@code other} so the
   * report stays schema-valid.
   *
   * @param sink the Spark write format used for the timed region
   * @return a schema-valid sink category
   */
  @Nonnull
  private static String schemaSink(@Nonnull final String sink) {
    return SCHEMA_SINKS.contains(sink) ? sink : "other";
  }

  @Nonnull
  private static ObjectNode caseNode(@Nonnull final CaseResult result) {
    final ObjectNode node = MAPPER.createObjectNode();
    node.put("title", result.getTitle());
    node.put("status", result.getStatus());
    node.put("inputRows", result.getInputRows());
    node.put("outputRows", result.getOutputRows());

    final List<Double> samples = result.getExecuteExtractSamplesMs();
    final ArrayNode samplesNode = node.putArray("samplesMs");
    samples.forEach(samplesNode::add);

    node.set("stats", statsNode(samples));

    final ObjectNode phaseSamples = node.putObject("phaseSamplesMs");
    phaseSamples.putArray("load").add(result.getLoadMs());
    final ArrayNode executeExtract = phaseSamples.putArray("executeExtract");
    samples.forEach(executeExtract::add);

    return node;
  }

  @Nonnull
  private static ObjectNode statsNode(@Nonnull final List<Double> samples) {
    final ObjectNode stats = MAPPER.createObjectNode();
    if (samples.isEmpty()) {
      return stats;
    }
    final List<Double> sorted = samples.stream().sorted().toList();
    final double min = sorted.get(0);
    final double max = sorted.get(sorted.size() - 1);
    final double mean = sorted.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    final double median = median(sorted);
    stats.put("min", min);
    stats.put("mean", mean);
    stats.put("median", median);
    stats.put("max", max);
    return stats;
  }

  private static double median(@Nonnull final List<Double> sorted) {
    final int size = sorted.size();
    final int mid = size / 2;
    if (size % 2 == 0) {
      return (sorted.get(mid - 1) + sorted.get(mid)) / 2.0;
    }
    return sorted.get(mid);
  }
}
