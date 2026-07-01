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
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
 * <p>It parses the benchmark file, locates its materialized NDJSON via {@link ManifestLocator},
 * loads each distinct subject once, measures every case over its subject, and writes a schema-valid
 * report.
 */
public final class SofBenchmarkRunner {

  @Nonnull private static final Logger log = LoggerFactory.getLogger(SofBenchmarkRunner.class);

  @Nonnull private static final String IMPLEMENTATION_NAME = "pathling-java";

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
    final int population = benchmark.getDataset().populationFor(size);
    final Path dataDir =
        ManifestLocator.locate(
            options.getDataRoot(), benchmark.getDataset().getName(), size, population);
    log.info("Located materialized data at {}", dataDir);

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

      final List<CaseResult> results = runCases(benchmark, size, ndjson, harness);

      ReportWriter.write(
          options.getOut(),
          IMPLEMENTATION_NAME,
          resolveVersion(),
          resolveBenchmarkVersion(options.getBenchmarkFile()),
          environment(spark),
          options.getSink(),
          benchmark.getWarmup(),
          benchmark.getMeasurement(),
          benchmark.getTitle(),
          size,
          benchmark.getFhirVersion(),
          results);
      log.info("Wrote benchmark report to {}", options.getOut());
    } finally {
      spark.stop();
      deleteRecursively(outDir);
    }
  }

  /**
   * Loads each distinct subject once (in case order of first appearance) then measures every case
   * over its loaded subject, returning the case results in benchmark-file order.
   */
  @Nonnull
  private static List<CaseResult> runCases(
      @Nonnull final BenchmarkFile benchmark,
      @Nonnull final String size,
      @Nonnull final QueryableDataSource ndjson,
      @Nonnull final MeasurementHarness harness) {
    final Map<String, LoadedSubject> loadedSubjects = new LinkedHashMap<>();
    final List<CaseResult> results = new ArrayList<>();
    for (final BenchmarkCase benchmarkCase : benchmark.getCases()) {
      final String subject = benchmarkCase.getResource();
      final LoadedSubject loaded =
          loadedSubjects.computeIfAbsent(subject, s -> harness.load(ndjson, s));
      results.add(harness.measure(loaded, subject, benchmarkCase, size));
    }
    return results;
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

  /**
   * Best-effort resolution of the benchmark submodule's git SHA by running {@code git rev-parse
   * HEAD} in the benchmark file's directory. Returns null when git is unavailable or the directory
   * is not a checkout.
   */
  @Nullable
  private static String resolveBenchmarkVersion(@Nonnull final Path benchmarkFile) {
    final Path directory = benchmarkFile.toAbsolutePath().getParent();
    if (directory == null) {
      return null;
    }
    try {
      final Process process =
          new ProcessBuilder("git", "-C", directory.toString(), "rev-parse", "HEAD")
              .redirectErrorStream(true)
              .start();
      final String output =
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      if (!process.waitFor(10, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        return null;
      }
      return process.exitValue() == 0 && !output.isEmpty() ? output : null;
    } catch (final IOException e) {
      return null;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
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
