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

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the implementation-agnostic SQL-on-FHIR benchmark with Pathling over the Java library-api.
 *
 * <p>The {@code sof-benchmark} module shades this class as its {@code mainClass}, so it runs
 * directly from the built jar:
 *
 * <pre>{@code
 * java -jar sof-benchmark-<ver>.jar \
 *   <benchmarkFile> --size s --data <dataRoot> --sink csv --out report-java.json
 * }</pre>
 *
 * <p>It parses the benchmark file, resolves its materialized NDJSON deterministically via {@link
 * DataLocator}, verifies the loaded files against the {@link Checkfile} sha256 lock, loads each
 * distinct subject once, measures every case over its subject, and writes a schema-valid report.
 */
public final class SofBenchmarkRunner {

  @Nonnull private static final Logger log = LoggerFactory.getLogger(SofBenchmarkRunner.class);

  /** The execution engine name recorded in {@code implementation.engine}. */
  @Nonnull private static final String ENGINE_NAME = "Pathling";

  /** The language binding name recorded in {@code implementation.binding}. */
  @Nonnull private static final String BINDING_NAME = "pathling-java";

  /** The measurement scenario: preloaded data, load excluded from the timed region. */
  @Nonnull private static final String SCENARIO = "preloaded_repeated";

  /** The sink categories permitted by {@code benchmark-report.schema.json}. */
  @Nonnull
  private static final Set<String> SCHEMA_SINKS = Set.of("table", "csv", "memory", "other");

  private SofBenchmarkRunner() {}

  /**
   * Entry point.
   *
   * @param args the command-line arguments
   */
  public static void main(@Nonnull final String[] args) {
    final RunnerOptions options = RunnerOptions.parse(args);

    final BenchmarkFile benchmark = BenchmarkFile.parse(options.getBenchmarkFile());
    final String size = options.getSize();
    // Validates that the size is declared for the dataset before resolving its data.
    benchmark.getDataset().populationFor(size);
    final Path dataDir =
        DataLocator.locate(
            options.getDataRoot(),
            benchmark.getDataset().getName(),
            benchmark.getDataset().getVersion(),
            size);
    log.info("Located materialized data at {}", dataDir);

    final Checkfile checkfile = Checkfile.parseOrEmpty(options.getBenchmarkFile());
    verifyDatasetIntegrity(dataDir, checkfile, size);

    final SparkSession spark = buildSpark();
    final Path outDir = createOutputDir();
    try {
      final PathlingContext pathlingContext = PathlingContext.create(spark);
      final QueryableDataSource ndjson = pathlingContext.read().ndjson(dataDir.toString());

      final MeasurementHarness harness =
          new MeasurementHarness(
              pathlingContext,
              benchmark.getWarmup(),
              benchmark.getMeasurement(),
              options.getSink(),
              outDir);

      final List<CaseResult> results = runCases(benchmark, size, checkfile, ndjson, harness);

      final String version = resolveVersion();
      ReportWriter.write(
          options.getOut(),
          ENGINE_NAME,
          version,
          BINDING_NAME,
          version,
          benchmark.getName(),
          benchmark.getVersion(),
          benchmark.getDataset().getName(),
          benchmark.getDataset().getVersion(),
          environment(spark),
          SCENARIO,
          schemaSink(options.getSink()),
          benchmark.getWarmup(),
          benchmark.getMeasurement(),
          size,
          benchmark.getFhirVersion(),
          checkfile.resourceCounts(size),
          results);
      log.info("Wrote benchmark report to {}", options.getOut());
    } finally {
      spark.stop();
      deleteRecursively(outDir);
    }
  }

  /**
   * Loads each distinct subject once (in case order of first appearance) then measures every case
   * over its loaded subject, returning the case results in benchmark-file order. The expected
   * output row count for each case is sourced from the checkfile by the case's stable id.
   */
  @Nonnull
  private static List<CaseResult> runCases(
      @Nonnull final BenchmarkFile benchmark,
      @Nonnull final String size,
      @Nonnull final Checkfile checkfile,
      @Nonnull final QueryableDataSource ndjson,
      @Nonnull final MeasurementHarness harness) {
    final Map<String, LoadedSubject> loadedSubjects = new LinkedHashMap<>();
    final List<CaseResult> results = new ArrayList<>();
    for (final BenchmarkCase benchmarkCase : benchmark.getCases()) {
      results.add(measureIsolated(benchmarkCase, size, checkfile, ndjson, harness, loadedSubjects));
    }
    return results;
  }

  /**
   * Measures one case under its own failure boundary (per-case failure isolation). A load,
   * preparation, or evaluation failure is recorded as an {@code execution_error} with an advisory
   * message and returned like any other outcome, so the caller's loop never aborts and never voids
   * the other cases' recorded results.
   */
  @Nonnull
  private static CaseResult measureIsolated(
      @Nonnull final BenchmarkCase benchmarkCase,
      @Nonnull final String size,
      @Nonnull final Checkfile checkfile,
      @Nonnull final QueryableDataSource ndjson,
      @Nonnull final MeasurementHarness harness,
      @Nonnull final Map<String, LoadedSubject> loadedSubjects) {
    return runIsolated(
        benchmarkCase.getId(),
        () -> {
          final String subject = benchmarkCase.getResource();
          final LoadedSubject loaded =
              loadedSubjects.computeIfAbsent(subject, s -> harness.load(ndjson, s));
          return harness.measure(
              loaded, subject, benchmarkCase, checkfile.expectedCount(benchmarkCase.getId(), size));
        });
  }

  /**
   * Runs a single case's measurement under a failure boundary (per-case failure isolation). When
   * the measurement raises, the failure is recorded as an {@code execution_error} with an advisory
   * message and returned like any other outcome, so the caller's loop never aborts and never voids
   * the other cases' recorded results.
   *
   * @param id the stable case id
   * @param measurement the measurement to run, which may raise
   * @return the measured result, or a failed result if the measurement raised
   */
  @Nonnull
  static CaseResult runIsolated(
      @Nonnull final String id, @Nonnull final Supplier<CaseResult> measurement) {
    try {
      return measurement.get();
    } catch (final RuntimeException e) {
      final String message = e.getMessage() == null ? e.toString() : e.getMessage();
      log.error("Case '{}' failed: {}", id, message, e);
      return CaseResult.failed(id, message);
    }
  }

  /**
   * Verifies each loaded NDJSON file against the checkfile's sha256 lock, logging any drift. When
   * no checkfile is present the run proceeds unverified with a clear warning.
   */
  private static void verifyDatasetIntegrity(
      @Nonnull final Path dataDir, @Nonnull final Checkfile checkfile, @Nonnull final String size) {
    if (!checkfile.isPresent()) {
      log.warn(
          "No checkfile beside the benchmark file; dataset integrity is unverified and output row"
              + " counts are not checked.");
      return;
    }
    final List<String> drift = ChecksumVerifier.verify(dataDir, checkfile.fileChecksums(size));
    if (drift.isEmpty()) {
      log.info("Dataset integrity verified against the checkfile for size {}.", size);
    } else {
      drift.forEach(message -> log.error("Dataset integrity: {}", message));
      log.error(
          "Loaded data does not match the checkfile lock; timings are NOT verified against the"
              + " locked dataset.");
    }
  }

  /**
   * Maps a Spark write format to a sink category permitted by the report schema. Formats such as
   * {@code parquet} that are not enumerated by the schema are reported as {@code other}.
   */
  @Nonnull
  private static String schemaSink(@Nonnull final String sink) {
    return SCHEMA_SINKS.contains(sink) ? sink : "other";
  }

  @Nonnull
  private static SparkSession buildSpark() {
    final int cores = Runtime.getRuntime().availableProcessors();
    return SparkSession.builder()
        .appName("SofBenchmarkRunner")
        .master("local[*]")
        // Keep the tiny SoF inputs from spawning ~200 shuffle part-files and adding scheduling
        // noise.
        .config("spark.sql.shuffle.partitions", String.valueOf(cores))
        .getOrCreate();
  }

  @Nonnull
  private static Path createOutputDir() {
    try {
      return Files.createTempDirectory("sof-benchmark-out-");
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to create output directory", e);
    }
  }

  @Nonnull
  private static ObjectNode environment(@Nonnull final SparkSession spark) {
    final ObjectNode environment = new ObjectMapper().createObjectNode();
    environment.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
    environment.put("java", System.getProperty("java.version"));
    environment.put("spark", spark.version());
    environment.put("cores", Runtime.getRuntime().availableProcessors());
    return environment;
  }

  @Nonnull
  private static String resolveVersion() {
    return new PathlingVersion().getBuildVersion().orElse("UNKNOWN");
  }

  private static void deleteRecursively(@Nonnull final Path directory) {
    try (final var paths = Files.walk(directory)) {
      paths
          .sorted((a, b) -> b.getNameCount() - a.getNameCount())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (final IOException e) {
                  log.warn("Failed to delete {}", path, e);
                }
              });
    } catch (final IOException e) {
      log.warn("Failed to clean up output directory {}", directory, e);
    }
  }
}
