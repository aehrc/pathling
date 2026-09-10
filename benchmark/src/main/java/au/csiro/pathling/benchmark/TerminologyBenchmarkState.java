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

package au.csiro.pathling.benchmark;

import static au.csiro.pathling.library.TerminologyHelpers.SNOMED_URI;
import static au.csiro.pathling.library.TerminologyHelpers.toCoding;
import static au.csiro.pathling.library.TerminologyHelpers.toEclValueSet;
import static au.csiro.pathling.sql.Terminology.member_of;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.library.PathlingContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * JMH state for the terminology benchmark. It builds a large synthetic dataset of SNOMED CT codings
 * and configures Pathling to evaluate {@code member_of} against an ECL-defined value set in one of
 * two modes: {@code local} (the executor-side index service reading a Delta terminology store) or
 * {@code remote} (a FHIR terminology server, with a client-side expansion cache).
 *
 * <p>The benchmark exists to characterise success criterion SC-003: local-mode {@code member_of}
 * over a large coding dataset should be at least as fast as remote mode with a warm cache. Both the
 * local store path and the remote server URL are supplied through system properties so the same
 * benchmark can run against any environment:
 *
 * <ul>
 *   <li>{@code pathling.benchmark.terminology.storagePath} - path to a terminology store already
 *       loaded with the SNOMED CT edition under test (required for {@code local} mode).
 *   <li>{@code pathling.benchmark.terminology.serverUrl} - base URL of the reference terminology
 *       server loaded with the same edition (required for {@code remote} mode).
 *   <li>{@code pathling.benchmark.terminology.ecl} - the ECL expression defining the value set
 *       (defaults to {@code << 73211009}, descendants-or-self of Diabetes mellitus).
 *   <li>{@code pathling.benchmark.terminology.rows} - the number of codings in the synthetic
 *       dataset (defaults to {@code 1000000}).
 * </ul>
 *
 * @author John Grimes
 */
@State(Scope.Benchmark)
public class TerminologyBenchmarkState {

  /** System property naming the local terminology store path. */
  static final String STORAGE_PATH_PROPERTY = "pathling.benchmark.terminology.storagePath";

  /** System property naming the reference terminology server URL. */
  static final String SERVER_URL_PROPERTY = "pathling.benchmark.terminology.serverUrl";

  /** System property naming the ECL expression defining the value set under test. */
  static final String ECL_PROPERTY = "pathling.benchmark.terminology.ecl";

  /** System property naming the synthetic dataset row count. */
  static final String ROWS_PROPERTY = "pathling.benchmark.terminology.rows";

  /** The default ECL expression: descendants-or-self of Diabetes mellitus. */
  private static final String DEFAULT_ECL = "<< 73211009";

  /** The default synthetic dataset row count. */
  private static final long DEFAULT_ROWS = 1_000_000L;

  /**
   * A fixed pool of SNOMED CT concepts used to populate the synthetic dataset. The pool
   * deliberately mixes concepts that fall inside the default value set (descendants of Diabetes
   * mellitus) with concepts that fall outside it, so that {@code member_of} returns a realistic
   * mixture of true and false results. All codes are drawn from the SNOMED CT International core
   * and are therefore present in any edition derived from it.
   */
  private static final List<String> CONCEPT_POOL =
      List.of(
          "73211009", // Diabetes mellitus.
          "46635009", // Type 1 diabetes mellitus.
          "44054006", // Type 2 diabetes mellitus.
          "111552007", // Diabetes mellitus without complication.
          "38341003", // Hypertensive disorder.
          "195967001", // Asthma.
          "22298006", // Myocardial infarction.
          "13645005", // Chronic obstructive lung disease.
          "84114007", // Heart failure.
          "396275006"); // Osteoarthritis.

  /** Selects the terminology backend to benchmark. */
  @Nullable
  @Param({"local", "remote"})
  private String mode;

  @Nullable private PathlingContext pathlingContext;
  @Nullable private Dataset<Row> codings;
  @Nullable private String valueSetUrl;

  /**
   * Setter for the benchmark mode parameter, invoked by the JMH framework.
   *
   * @param mode the terminology backend to benchmark ({@code local} or {@code remote})
   */
  @SuppressWarnings("unused")
  public void setMode(@Nullable final String mode) {
    this.mode = mode;
  }

  /**
   * Prepares the Spark session, the mode-specific Pathling context, and the cached synthetic
   * dataset once per trial, then warms the terminology backend by evaluating the query a single
   * time. Warming loads the local indexes (or populates the remote expansion cache) so that the
   * measured iterations reflect steady-state, per-row evaluation cost rather than one-off setup.
   */
  @Setup(Level.Trial)
  public void setup() {
    final SparkSession session =
        SparkSession.builder()
            .appName("TerminologyBenchmark")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    this.pathlingContext =
        PathlingContext.createForTerminology(session, terminologyConfiguration());
    this.valueSetUrl = toEclValueSet(property(ECL_PROPERTY, DEFAULT_ECL));
    this.codings = buildDataset(session);
    // Warm the backend so measured iterations exclude index loading / cache population.
    evaluate();
  }

  /**
   * Builds the mode-specific terminology configuration. Local mode points at the pre-loaded store;
   * remote mode points at the reference server with its default (enabled) client-side cache.
   *
   * @return the terminology configuration for the current mode
   */
  @Nonnull
  private TerminologyConfiguration terminologyConfiguration() {
    if ("local".equals(mode)) {
      final String storagePath = requiredProperty(STORAGE_PATH_PROPERTY);
      return TerminologyConfiguration.builder()
          .mode(TerminologyMode.LOCAL)
          .local(LocalTerminologyConfiguration.builder().storagePath(storagePath).build())
          .build();
    } else if ("remote".equals(mode)) {
      return TerminologyConfiguration.builder()
          .mode(TerminologyMode.SERVER)
          .serverUrl(requiredProperty(SERVER_URL_PROPERTY))
          .build();
    }
    throw new IllegalArgumentException("Unknown terminology benchmark mode: " + mode);
  }

  /**
   * Builds and caches the synthetic dataset of SNOMED CT codings. Codes are drawn from {@link
   * #CONCEPT_POOL} in a round-robin fashion so the mixture of members and non-members is
   * deterministic and independent of the row count.
   *
   * @param session the Spark session
   * @return a cached, materialised dataset with a single {@code coding} struct column
   */
  @Nonnull
  private Dataset<Row> buildDataset(@Nonnull final SparkSession session) {
    final long rows = Long.parseLong(property(ROWS_PROPERTY, Long.toString(DEFAULT_ROWS)));
    final Column pool =
        array(
            CONCEPT_POOL.stream().map(org.apache.spark.sql.functions::lit).toArray(Column[]::new));
    // element_at is 1-based, so map n modulo the pool size into the range [1, poolSize].
    final Column code =
        element_at(pool, col("n").mod(lit(CONCEPT_POOL.size())).plus(lit(1)).cast("int"));
    final Dataset<Row> dataset =
        session
            .range(rows)
            .toDF("n")
            .withColumn("coding", toCoding(code, SNOMED_URI, null))
            .select("coding")
            .cache();
    dataset.count();
    return dataset;
  }

  /**
   * Evaluates {@code member_of} over the synthetic dataset and counts the members, forcing full
   * execution. Called by the benchmark method and once during setup to warm the backend.
   *
   * @return the number of codings that are members of the value set
   */
  public long evaluate() {
    final Dataset<Row> dataset = requireCodings();
    return dataset
        .select(member_of(dataset.col("coding"), requireValueSetUrl()).alias("member"))
        .where(col("member"))
        .count();
  }

  /** Stops the Spark session at the end of the trial to release resources. */
  @TearDown(Level.Trial)
  public void teardown() {
    if (pathlingContext != null) {
      pathlingContext.getSpark().stop();
    }
  }

  @Nonnull
  private Dataset<Row> requireCodings() {
    if (codings == null) {
      throw new IllegalStateException("Dataset not initialised; ensure setup() has run.");
    }
    return codings;
  }

  @Nonnull
  private String requireValueSetUrl() {
    if (valueSetUrl == null) {
      throw new IllegalStateException("Value set URL not initialised; ensure setup() has run.");
    }
    return valueSetUrl;
  }

  @Nonnull
  private static String property(@Nonnull final String name, @Nonnull final String defaultValue) {
    final String value = System.getProperty(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  @Nonnull
  private static String requiredProperty(@Nonnull final String name) {
    final String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          "Required benchmark system property is not set: -D" + name + "=<value>");
    }
    return value;
  }
}
