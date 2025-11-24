/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 * JMH (Java Microbenchmark Harness) state class for Pathling benchmarking.
 * <p>
 * This class manages the setup and configuration of benchmark resources for testing Pathling's
 * performance with different data sources and formats. It provides a standardized environment for
 * running performance tests against FHIR data using both NDJSON and Delta Lake storage formats.
 * <p>
 * The class is annotated with @State(Scope.Benchmark) to ensure that the same instance is shared
 * across all benchmark iterations, providing consistent test conditions while amortising expensive
 * setup costs.
 *
 * @author John Grimes
 */
@State(Scope.Benchmark)
public class PathlingBenchmarkState {

  /**
   * Pre-defined view definitions used in benchmarks to test query scenarios. These view definitions
   * are taken from the SQL on FHIR implementation guide.
   */
  private static final List<String> VIEW_DEFINITIONS = List.of(
      "ConditionFlat", "EncounterFlat", "PatientAddresses", "PatientAndContactAddressUnion",
      "PatientDemographics", "UsCoreBloodPressures", "QuestionnaireResponseFlat"
  );

  /**
   * File extension for JSON view definition files stored as resources
   */
  private static final String JSON_EXTENSION = ".json";

  /**
   * The main Pathling context that wraps the Spark session and provides FHIR-specific functionality
   * including encoding, querying, and data transformation operations.
   */
  @Nonnull
  private final PathlingContext pathlingContext;

  /**
   * The data source containing FHIR resources for benchmark testing. This can be either an
   * NDJSON-based source or a Delta Lake source, depending on the benchmark configuration.
   * Initialized during setup phase.
   */
  @Nullable
  private QueryableDataSource dataSource;

  /**
   * Cached view definitions loaded from JSON resources. Maps view names to their JSON string
   * representations for use in benchmark queries. Populated during the setup phase to avoid I/O
   * overhead during actual benchmarks.
   */
  @Nullable
  private Map<String, String> viewDefinitions;

  /**
   * JMH parameter that controls which data source type to use for benchmarking. Supports two
   * values: - "ndjson": Uses newline-delimited JSON files as the data source - "delta": Uses Delta
   * Lake tables for optimized columnar storage and querying
   * <p>
   * This parameter allows benchmarks to compare performance between different storage formats under
   * identical conditions.
   */
  @Nullable
  @Param({"ndjson", "delta"})
  private String sourceType;

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
   * Constructor initializes the core Spark session and Pathling context.
   * <p>
   * Sets up Apache Spark with the following configurations: - Local execution mode using all
   * available CPU cores - Delta Lake SQL extensions for advanced data lake capabilities - Delta
   * catalog integration for table management
   * <p>
   * The Pathling context wraps this Spark session to provide FHIR-specific data processing
   * capabilities including resource encoding and FHIRPath queries.
   */
  @SuppressWarnings("ConstantValue")
  public PathlingBenchmarkState() {
    // Configure Spark session with Delta Lake support for high-performance analytics
    final SparkSession spark = SparkSession.builder()
        .appName("PathlingBenchmark")
        .master("local[*]") // Use all available CPU cores for maximum performance
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    // Create Pathling context with the configured Spark session
    this.pathlingContext = PathlingContext.create(spark);
  }

  /**
   * JMH setup method executed once per benchmark trial to prepare test data and resources.
   * <p>
   * This method performs expensive initialization that should be amortized across all benchmark
   * iterations: 1. Loads FHIR test data in the specified format (NDJSON or Delta) 2. Pre-loads and
   * caches view definitions from JSON resources 3. Validates that all required resources are
   * available
   * <p>
   * The setup is parameterized by sourceType to enable performance comparisons between different
   * storage formats under identical conditions.
   *
   * @throws IllegalArgumentException if an unsupported source type is specified
   * @throws RuntimeException if required resources cannot be loaded
   */
  @Setup(Level.Trial)
  public void setup() {
    // Define the FHIR resource types that will be loaded for benchmarking
    // These represent core clinical data types commonly used in healthcare analytics
    final List<String> resourceTypes = List.of("Patient", "Observation", "Condition", "Encounter",
        "QuestionnaireResponse");

    // Initialize the appropriate data source based on the benchmark parameter
    if ("ndjson".equals(sourceType)) {
      // Use NDJSON files for row-oriented processing and simple data access
      this.dataSource = initialiseNdjsonSource(resourceTypes);
    } else if ("delta".equals(sourceType)) {
      // Use Delta Lake for columnar storage, ACID transactions, and optimized queries
      this.dataSource = initialiseDeltaSource(resourceTypes);
    } else {
      throw new IllegalArgumentException("Unknown source type: " + sourceType);
    }

    // Pre-load view definitions to avoid I/O overhead during benchmark execution
    // Each view definition is loaded from a JSON resource file and cached in memory
    this.viewDefinitions = VIEW_DEFINITIONS.stream()
        .collect(toMap(
            viewDefaultName -> viewDefaultName,
            viewDefaultName -> {
              try (final InputStream in = getResourceAsStream(viewDefaultName + JSON_EXTENSION)) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
              } catch (final IOException e) {
                throw new UncheckedIOException("Failed to read view definition: " + viewDefaultName,
                    e);
              }
            }
        ));
  }

  /**
   * Initializes a dataset source from NDJSON files containing FHIR resources.
   * <p>
   * This method creates an in-memory dataset source by: 1. Extracting NDJSON files from JAR
   * resources to temporary files 2. Loading each file as a text dataset in Spark 3. Encoding the
   * JSON strings into Pathling's internal FHIR representation 4. Registering each encoded dataset
   * with the data source
   *
   * @param resourceTypes The FHIR resource types to load (e.g., Patient, Observation)
   * @return A configured DatasetSource containing the encoded FHIR data
   * @throws RuntimeException if resource files cannot be loaded or encoded
   */
  private @Nonnull DatasetSource initialiseNdjsonSource(
      @Nonnull final Iterable<String> resourceTypes) {
    // Create a DatasetSource.
    final DatasetSource datasetSource = pathlingContext.read().datasets();
    for (final String resourceType : resourceTypes) {
      // Load the NDJSON file for the resource type and encode it.
      final Path ndjsonPath = extractResourceToTempFile("bulk/fhir/" + resourceType + ".ndjson");
      final Dataset<Row> strings = this.pathlingContext.getSpark().read().format("text")
          .load(ndjsonPath.toString());
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
   * @throws RuntimeException if temporary directory creation or Delta writing fails
   */
  @Nonnull
  private QueryableDataSource initialiseDeltaSource(@Nonnull final Iterable<String> resourceTypes) {
    // First, create datasets from NDJSON files using the standard process
    final DatasetSource datasetSource = initialiseNdjsonSource(resourceTypes);

    // Create a temporary directory for the Delta tables.
    final Path tempDir;
    try {
      tempDir = Files.createTempDirectory("pathling-benchmark-delta-");
      // Ensure cleanup on JVM exit to prevent disk space issues
      tempDir.toFile().deleteOnExit();
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to create temporary directory for Delta tables", e);
    }

    // Write each dataset to a Delta table in the temporary directory.
    // This converts the in-memory datasets to optimized Delta Lake format
    datasetSource.write().delta(tempDir.toString());

    // Create a DeltaSource that reads from the temporary directory.
    // This provides access to the Delta tables for benchmark queries
    return pathlingContext.read().delta(tempDir.toString());
  }

  /**
   * Utility method to get the current thread's context class loader. Used for loading resources
   * from the application's classpath.
   *
   * @return The context class loader for resource loading
   * @throws NullPointerException if no context class loader is available
   */
  @Nonnull
  private static ClassLoader getClassLoader() {
    final ClassLoader object = Thread.currentThread().getContextClassLoader();
    return requireNonNull(object);
  }

  /**
   * Loads a resource file as an InputStream from the application's classpath. Used to access test
   * data and configuration files packaged within the JAR.
   *
   * @param name The name/path of the resource to load
   * @return An InputStream for reading the resource content
   * @throws NullPointerException if the resource cannot be found
   */
  @Nonnull
  private static InputStream getResourceAsStream(@Nonnull final String name) {
    final ClassLoader loader = getClassLoader();
    final InputStream inputStream = loader.getResourceAsStream(name);
    requireNonNull(inputStream, "Test resource not found: " + name);
    return inputStream;
  }

  /**
   * Extracts a resource from the JAR to a temporary file on disk.
   * <p>
   * This is necessary because some operations (like Spark file reading) require actual file paths
   * rather than classpath resources. The method: 1. Creates a temporary file with a unique name 2.
   * Copies the resource content to the temporary file 3. Marks the file for deletion on JVM exit
   *
   * @param resourceName The name/path of the resource to extract
   * @return A Path pointing to the temporary file containing the resource content
   * @throws RuntimeException if the extraction process fails
   */
  @Nonnull
  private static Path extractResourceToTempFile(@Nonnull final String resourceName) {
    try (final InputStream in = getResourceAsStream(resourceName)) {
      // Create a temporary file with a descriptive name for debugging
      final Path tempFile = Files.createTempFile("pathling-benchmark-",
          "-" + resourceName.replace('/', '_'));
      // Ensure cleanup on JVM exit to prevent disk space accumulation
      tempFile.toFile().deleteOnExit();

      // Copy resource content to the temporary file
      try (final OutputStream out = Files.newOutputStream(tempFile)) {
        in.transferTo(out);
      }
      return tempFile;
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to extract resource: " + resourceName, e);
    }
  }

  /**
   * Provides access to the initialized data source for benchmark queries.
   *
   * @return The QueryableDataSource containing FHIR test data
   * @throws IllegalStateException if called before setup() has initialized the data source
   */
  @Nonnull
  public QueryableDataSource getNdjsonSource() {
    if (dataSource == null) {
      throw new IllegalStateException("Data source not initialized. Ensure setup() is called.");
    }
    return dataSource;
  }

  /**
   * Provides access to the pre-loaded view definitions for benchmark queries.
   *
   * @return A map of view names to their JSON definition strings
   * @throws IllegalStateException if called before setup() has loaded the view definitions
   */
  @Nonnull
  public Map<String, String> getViewDefinitions() {
    if (viewDefinitions == null) {
      throw new IllegalStateException(
          "View definitions not initialized. Ensure setup() is called.");
    }
    return viewDefinitions;
  }

  /**
   * JMH teardown method executed once per benchmark trial to clean up resources.
   * <p>
   * This method ensures proper cleanup of the Spark session to prevent resource leaks and conflicts
   * between benchmark runs. The Spark session holds significant system resources including thread
   * pools, memory caches, and network connections that must be properly released.
   * <p>
   * Note: Temporary files are automatically cleaned up via deleteOnExit() calls made during their
   * creation, so no explicit file cleanup is needed here.
   */
  @TearDown(Level.Trial)
  public void teardown() {
    // Stop the Spark session to release all associated resources
    // This includes thread pools, memory caches, and network connections
    pathlingContext.getSpark().stop();
  }

}
