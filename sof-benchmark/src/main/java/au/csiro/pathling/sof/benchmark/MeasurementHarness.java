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

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the load phase and the timed execute+extract loop for benchmark cases.
 *
 * <p>The load phase reads and FHIR-encodes a subject's NDJSON, caches it, and forces its
 * materialization with a {@code count()} so that the timed region never absorbs parse/encode cost.
 * The timed region evaluates the view and writes the full result to a fixed temp directory with
 * {@link SaveMode#Overwrite}; the output row count is obtained outside the timed region. Warmup
 * iterations run in the same JVM so whole-stage codegen is cached before the measured iterations.
 */
public class MeasurementHarness {

  @Nonnull private static final Logger log = LoggerFactory.getLogger(MeasurementHarness.class);

  private static final double NANOS_PER_MILLI = 1_000_000.0;

  @Nonnull private final PathlingContext pathlingContext;

  private final int warmup;

  private final int iterations;

  @Nonnull private final String sink;

  @Nonnull private final Path outDir;

  /**
   * Constructs a measurement harness.
   *
   * @param pathlingContext the Pathling context used to rebuild a cached dataset source
   * @param warmup the number of warmup iterations to discard
   * @param iterations the number of measured iterations to record
   * @param sink the Spark write format (e.g. {@code csv} or {@code parquet})
   * @param outDir the fixed temporary directory the results are written to
   */
  public MeasurementHarness(
      @Nonnull final PathlingContext pathlingContext,
      final int warmup,
      final int iterations,
      @Nonnull final String sink,
      @Nonnull final Path outDir) {
    this.pathlingContext = pathlingContext;
    this.warmup = warmup;
    this.iterations = iterations;
    this.sink = sink;
    this.outDir = outDir;
  }

  /**
   * Performs the load phase for a subject: reads and FHIR-encodes the subject's data, caches it,
   * and forces materialization to populate the cache and capture the input row count.
   *
   * @param ndjson the NDJSON-backed data source covering all the benchmark's resources
   * @param subject the subject resource type to load
   * @return the cached subject with its input row count and load duration
   */
  @Nonnull
  public LoadedSubject load(
      @Nonnull final QueryableDataSource ndjson, @Nonnull final String subject) {
    log.info("Loading subject {}", subject);
    final long start = System.nanoTime();
    // DataSource.read(String) returns the lazy FHIR-encoded dataset; caching plus count() forces
    // the
    // parse+encode to happen now so the timed region measures only view evaluation and write.
    final Dataset<Row> encoded = ndjson.read(subject).cache();
    final long inputRows = encoded.count();
    final double loadMs = (System.nanoTime() - start) / NANOS_PER_MILLI;

    // Rebuild as a DatasetSource over the already-cached dataset so view(subject) is available.
    final DatasetSource cached = pathlingContext.read().datasets().dataset(subject, encoded);
    log.info("Loaded subject {}: {} input rows in {} ms", subject, inputRows, loadMs);
    return new LoadedSubject(cached, inputRows, loadMs);
  }

  /**
   * Measures a single case over its already-loaded subject: runs the discarded warmup iterations,
   * then the measured iterations timing each execute+write, and finally obtains the output row
   * count outside the timed region to compute the correctness status.
   *
   * @param loaded the cached subject the case runs over
   * @param subject the subject resource type
   * @param benchmarkCase the case to measure
   * @param expectedCount the checkfile assertion for this case and size, if any
   * @return the measured case result
   */
  @Nonnull
  public CaseResult measure(
      @Nonnull final LoadedSubject loaded,
      @Nonnull final String subject,
      @Nonnull final BenchmarkCase benchmarkCase,
      @Nonnull final Optional<Integer> expectedCount) {
    final String viewJson = benchmarkCase.getViewJson();
    log.info(
        "Measuring case '{}' ({} warmup, {} measured)", benchmarkCase.getId(), warmup, iterations);

    for (int i = 0; i < warmup; i++) {
      writeOnce(loaded.getSource(), subject, viewJson);
    }

    final List<Double> samples = new ArrayList<>();
    Dataset<Row> lastResult = null;
    for (int i = 0; i < iterations; i++) {
      final Dataset<Row> result = loaded.getSource().view(subject).json(viewJson).execute();
      final long start = System.nanoTime();
      result.write().format(sink).mode(SaveMode.Overwrite).save(outDir.toString());
      samples.add((System.nanoTime() - start) / NANOS_PER_MILLI);
      lastResult = result;
    }

    // Untimed correctness guard: count() never times the measured region because Catalyst would
    // prune projected columns and under-measure.
    final long outputRows = lastResult == null ? 0L : lastResult.count();
    final String status =
        CorrectnessGuard.status(
            expectedCount, outputRows, benchmarkCase.isCountVariancePermitted());
    log.info("Case '{}': {} output rows, status {}", benchmarkCase.getId(), outputRows, status);

    return new CaseResult(
        benchmarkCase.getId(),
        status,
        loaded.getInputRows(),
        outputRows,
        samples,
        loaded.getLoadMs());
  }

  private void writeOnce(
      @Nonnull final DatasetSource source,
      @Nonnull final String subject,
      @Nonnull final String viewJson) {
    final Dataset<Row> result = source.view(subject).json(viewJson).execute();
    result.write().format(sink).mode(SaveMode.Overwrite).save(outDir.toString());
  }
}
