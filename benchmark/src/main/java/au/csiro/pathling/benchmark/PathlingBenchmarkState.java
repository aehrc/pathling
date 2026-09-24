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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * JMH (Java Microbenchmark Harness) state class for the view query benchmarks.
 *
 * <p>This class prepares a single view definition over a single resource type, and exposes the two
 * halves of running that view separately: building the query plan, and executing a plan that has
 * already been built. Encoding and decoding are measured by {@link EncodeBenchmarkState} and {@link
 * DecodeBenchmarkState} respectively, so that a query figure measures querying alone.
 *
 * <p>The class is annotated with @State(Scope.Benchmark) to ensure that the same instance is shared
 * across all benchmark iterations, providing consistent test conditions while amortising expensive
 * setup costs.
 *
 * @author John Grimes
 */
@State(Scope.Benchmark)
public class PathlingBenchmarkState {

  /**
   * The resource type that each benchmarked view definition selects over. None of these views
   * follows a reference to another resource, so only the subject resource needs to be loaded.
   */
  private static final Map<String, String> VIEW_SUBJECTS =
      Map.of(
          "ConditionFlat", "Condition",
          "EncounterFlat", "Encounter",
          "PatientAddresses", "Patient",
          "PatientAndContactAddressUnion", "Patient",
          "PatientDemographics", "Patient",
          "UsCoreBloodPressures", "Observation",
          "QuestionnaireResponseFlat", "QuestionnaireResponse");

  /** File extension for JSON view definition files stored as resources. */
  private static final String JSON_EXTENSION = ".json";

  /**
   * The main Pathling context that wraps the Spark session and provides FHIR-specific functionality
   * including encoding, querying, and data transformation operations.
   */
  @Nonnull private final PathlingContext pathlingContext;

  /**
   * The data source containing FHIR resources for benchmark testing. This can be either an
   * NDJSON-based source or a Delta Lake source, depending on the benchmark configuration.
   * Initialized during setup phase.
   */
  @Nullable private QueryableDataSource dataSource;

  /**
   * The JSON representation of the view definition selected by the {@link #view} parameter, loaded
   * during setup so that the benchmarks do not pay for the I/O.
   */
  @Nullable private String viewDefinition;

  /**
   * A query plan built ahead of the operation that executes it, so that the execution benchmark
   * measures execution alone. A fresh plan is built for every invocation, because re-executing one
   * plan would reuse the query stages that the first execution materialised.
   */
  @Nullable private Dataset<Row> preparedQuery;

  /**
   * JMH parameter that controls which data source type to use for benchmarking. Supports two
   * values: - "ndjson": Uses newline-delimited JSON files as the data source - "delta": Uses Delta
   * Lake tables for optimized columnar storage and querying
   *
   * <p>Only "delta" is exercised. An NDJSON source holds a lazy encode over the raw text, so a
   * query over it measures encoding as well as querying, which is the conflation that these
   * benchmarks exist to remove; encoding now has its own benchmark. Adding "ndjson" back to this
   * list restores the earlier behaviour.
   */
  @Nullable
  @Param({"delta"})
  private String sourceType;

  /**
   * JMH parameter that selects the view definition to benchmark. The values are the keys of {@link
   * #VIEW_SUBJECTS}, which they must be kept in step with, as an annotation cannot refer to the
   * map.
   */
  @Nullable
  @Param({
    "ConditionFlat",
    "EncounterFlat",
    "PatientAddresses",
    "PatientAndContactAddressUnion",
    "PatientDemographics",
    "UsCoreBloodPressures",
    "QuestionnaireResponseFlat"
  })
  private String view;

  /**
   * Setter for the source type parameter. Used by JMH framework to inject parameter values during
   * benchmark execution.
   *
   * @param sourceType The type of data source to use ("ndjson" or "delta")
   */
  @SuppressWarnings("unused")
  public void setSourceType(@Nullable final String sourceType) {
    this.sourceType = sourceType;
  }

  /**
   * Setter for the view parameter. Used by JMH framework to inject parameter values during
   * benchmark execution.
   *
   * @param view the name of the view definition to benchmark
   */
  @SuppressWarnings("unused")
  public void setView(@Nullable final String view) {
    this.view = view;
  }

  /**
   * Constructor initializes the core Spark session and Pathling context.
   *
   * <p>The Pathling context wraps this Spark session to provide FHIR-specific data processing
   * capabilities including resource encoding and FHIRPath queries.
   */
  public PathlingBenchmarkState() {
    this.pathlingContext = BenchmarkResources.createPathlingContext("PathlingBenchmark");
  }

  /**
   * JMH setup method executed once per benchmark trial to prepare test data and resources.
   *
   * <p>This method performs expensive initialization that should be amortized across all benchmark
   * iterations: it loads the subject resource of the selected view in the specified format, and
   * loads the view definition itself. Only the subject resource is loaded, because the cost of
   * loading a resource type that the view does not read would fall entirely within setup and serve
   * no purpose.
   *
   * @throws IllegalArgumentException if an unsupported source type is specified
   */
  @Setup(Level.Trial)
  public void setup() {
    final String subjectResource = getSubjectResource();

    // Initialize the appropriate data source based on the benchmark parameter.
    if ("ndjson".equals(sourceType)) {
      // Use NDJSON files for row-oriented processing and simple data access.
      this.dataSource = initialiseNdjsonSource(List.of(subjectResource));
    } else if ("delta".equals(sourceType)) {
      // Use Delta Lake for columnar storage, ACID transactions, and optimized queries.
      this.dataSource = initialiseDeltaSource(List.of(subjectResource));
    } else {
      throw new IllegalArgumentException("Unknown source type: " + sourceType);
    }

    // Pre-load the view definition to avoid I/O overhead during benchmark execution.
    try (final InputStream in =
        BenchmarkResources.getResourceAsStream(requireNonNull(view) + JSON_EXTENSION)) {
      this.viewDefinition = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to read view definition: " + view, e);
    }
  }

  /**
   * Builds a query plan before each invocation, so that the execution benchmark can measure
   * execution without the planning that precedes it.
   *
   * <p>This runs before every invocation of every benchmark that takes this state, including the
   * planning benchmark, where its only effect is to reduce the number of samples taken within an
   * iteration. JMH excludes the time it takes from the measurement.
   */
  @Setup(Level.Invocation)
  public void prepareQuery() {
    this.preparedQuery = planQuery();
  }

  /**
   * Builds the query plan for the selected view, up to and including the physical plan.
   *
   * <p>Constructing the dataset runs the FHIRPath translation and the Catalyst analyzer. Forcing
   * the physical plan additionally runs the optimiser and the planner, so that everything which
   * precedes the submission of a Spark job falls on the planning side of the split. No job is
   * submitted, because Spark plans lazily and no action is taken here.
   *
   * @return the planned dataset, ready to be executed
   */
  @Nonnull
  public Dataset<Row> planQuery() {
    final Dataset<Row> result =
        getDataSource().view(getSubjectResource()).json(getViewDefinition()).execute();
    // The physical plan is lazy, and this statement is what forces it to be built.
    result.queryExecution().executedPlan();
    return result;
  }

  /**
   * Executes the plan built by {@link #prepareQuery()} and collects the result.
   *
   * <p>The plan is built fresh for each invocation and is never cached or persisted, so this runs
   * the whole query rather than reusing a result or a materialised query stage.
   *
   * @return the collected rows
   */
  @Nonnull
  public List<Row> executePreparedQuery() {
    return requireNonNull(preparedQuery, "Query not prepared. Ensure prepareQuery() is called.")
        .collectAsList();
  }

  /**
   * Initializes a dataset source from NDJSON files containing FHIR resources.
   *
   * <p>This method creates an in-memory dataset source by: 1. Extracting NDJSON files from JAR
   * resources to temporary files 2. Loading each file as a text dataset in Spark 3. Encoding the
   * JSON strings into Pathling's internal FHIR representation 4. Registering each encoded dataset
   * with the data source
   *
   * <p>The encoding is lazy, so a query over the resulting source encodes the data as part of the
   * query.
   *
   * @param resourceTypes The FHIR resource types to load (e.g., Patient, Observation)
   * @return A configured DatasetSource containing the encoded FHIR data
   */
  @Nonnull
  private DatasetSource initialiseNdjsonSource(@Nonnull final Iterable<String> resourceTypes) {
    // Create a DatasetSource.
    final DatasetSource datasetSource = pathlingContext.read().datasets();
    for (final String resourceType : resourceTypes) {
      // Load the NDJSON file for the resource type and encode it.
      final Dataset<Row> strings =
          BenchmarkResources.readResourceStrings(pathlingContext, resourceType);
      final Dataset<Row> encoded = pathlingContext.encode(strings, resourceType);
      // Register the dataset with the DatasetSource.
      datasetSource.dataset(resourceType, encoded);
    }
    return datasetSource;
  }

  /**
   * Initializes a Delta Lake source by first creating datasets from NDJSON files, then writing them
   * to Delta tables for optimized storage and querying.
   *
   * @param resourceTypes The FHIR resource types to load and convert to Delta format
   * @return A DeltaSource configured to read from temporary Delta tables
   */
  @Nonnull
  private QueryableDataSource initialiseDeltaSource(@Nonnull final Iterable<String> resourceTypes) {
    // First, create datasets from NDJSON files using the standard process.
    final DatasetSource datasetSource = initialiseNdjsonSource(resourceTypes);

    // Create a temporary directory for the Delta tables.
    final Path tempDir = BenchmarkResources.createTempDirectory("pathling-benchmark-delta-");

    // Write each dataset to a Delta table in the temporary directory.
    // This converts the in-memory datasets to optimized Delta Lake format.
    datasetSource.write().delta(tempDir.toString());

    // Create a DeltaSource that reads from the temporary directory.
    // This provides access to the Delta tables for benchmark queries.
    return pathlingContext.read().delta(tempDir.toString());
  }

  /**
   * Provides access to the initialized data source for benchmark queries.
   *
   * @return The QueryableDataSource containing FHIR test data
   * @throws IllegalStateException if called before setup() has initialized the data source
   */
  @Nonnull
  public QueryableDataSource getDataSource() {
    if (dataSource == null) {
      throw new IllegalStateException("Data source not initialized. Ensure setup() is called.");
    }
    return dataSource;
  }

  /**
   * Provides access to the pre-loaded definition of the selected view.
   *
   * @return the JSON representation of the view definition
   * @throws IllegalStateException if called before setup() has loaded the view definition
   */
  @Nonnull
  public String getViewDefinition() {
    if (viewDefinition == null) {
      throw new IllegalStateException("View definition not initialized. Ensure setup() is called.");
    }
    return viewDefinition;
  }

  /**
   * Resolves the resource type that the selected view selects over.
   *
   * @return the subject resource type of the selected view
   */
  @Nonnull
  private String getSubjectResource() {
    return requireNonNull(VIEW_SUBJECTS.get(view), "Unknown view: " + view);
  }

  /**
   * JMH teardown method executed once per benchmark trial to clean up resources.
   *
   * <p>This method ensures proper cleanup of the Spark session to prevent resource leaks and
   * conflicts between benchmark runs. The Spark session holds significant system resources
   * including thread pools, memory caches, and network connections that must be properly released.
   *
   * <p>Note: Temporary files are automatically cleaned up via deleteOnExit() calls made during
   * their creation, so no explicit file cleanup is needed here.
   */
  @TearDown(Level.Trial)
  public void teardown() {
    // Stop the Spark session to release all associated resources.
    pathlingContext.getSpark().stop();
  }
}
