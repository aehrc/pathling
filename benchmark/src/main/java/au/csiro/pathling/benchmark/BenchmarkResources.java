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
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Shared setup helpers for the benchmark state classes.
 *
 * <p>Each phase of the pipeline is measured by its own state class, so that a parameter which is
 * only meaningful to one phase does not multiply the fork count of the others. The construction of
 * the Spark session, the extraction of test data from the classpath and the forcing of lazy Spark
 * computation are common to all of them, and live here rather than being repeated.
 *
 * @author Piotr Szul
 */
public final class BenchmarkResources {

  /** The directory within the test data archive that holds the bulk FHIR NDJSON files. */
  private static final String NDJSON_PREFIX = "bulk/fhir/";

  /** The file extension of the bulk FHIR NDJSON files. */
  private static final String NDJSON_EXTENSION = ".ndjson";

  /**
   * The Spark data source that runs a query to completion and discards every row. It is used to
   * force lazy computation without adding the cost of a sink, and without allowing the optimiser to
   * prune any of the columns that the benchmark exists to measure.
   */
  private static final String NOOP_FORMAT = "noop";

  private BenchmarkResources() {
    // This class is not designed to be instantiated.
  }

  /**
   * Creates a Pathling context backed by a local Spark session with Delta Lake support.
   *
   * @param appName the Spark application name, used to tell the benchmark sessions apart in logs
   * @return a Pathling context wrapping the new session
   */
  @Nonnull
  public static PathlingContext createPathlingContext(@Nonnull final String appName) {
    // Configure Spark session with Delta Lake support for high-performance analytics.
    final SparkSession spark =
        SparkSession.builder()
            .appName(appName)
            .master("local[*]") // Use all available CPU cores for maximum performance.
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    return PathlingContext.create(spark);
  }

  /**
   * Reads the bulk NDJSON test data for a resource type into a single-column dataset of strings.
   *
   * <p>The read itself is lazy, so the cost of reading the file falls within the operation that
   * consumes the dataset rather than within setup. That is deliberate for the encoding benchmark,
   * where reading the file is part of what encoding a bulk export costs.
   *
   * @param pathlingContext the context supplying the Spark session
   * @param resourceType the FHIR resource type to read
   * @return a dataset with one row per resource, each holding the JSON representation
   */
  @Nonnull
  public static Dataset<Row> readResourceStrings(
      @Nonnull final PathlingContext pathlingContext, @Nonnull final String resourceType) {
    final Path ndjsonPath =
        extractResourceToTempFile(NDJSON_PREFIX + resourceType + NDJSON_EXTENSION);
    return pathlingContext.getSpark().read().format("text").load(ndjsonPath.toString());
  }

  /**
   * Runs a dataset to completion and discards the result.
   *
   * <p>Spark is lazy, so a benchmark that only builds a dataset measures the construction of a
   * query plan rather than the work that the plan describes. The {@code noop} data source forces
   * the plan to run without the cost of a real sink and without returning anything to the driver.
   *
   * <p>A row count is not used for this purpose, because the optimiser is free to prune the fields
   * of an object serializer that nothing downstream reads. Counting the rows of an encoded dataset
   * would therefore skip the construction of the very columns whose width these benchmarks measure,
   * whereas a write consumes the full schema.
   *
   * @param dataset the dataset to run
   */
  public static void materialise(@Nonnull final Dataset<?> dataset) {
    dataset.write().format(NOOP_FORMAT).mode(SaveMode.Overwrite).save();
  }

  /**
   * Creates a temporary directory that is removed when the JVM exits.
   *
   * @param prefix the prefix of the directory name
   * @return the path of the new directory
   */
  @Nonnull
  public static Path createTempDirectory(@Nonnull final String prefix) {
    try {
      final Path tempDir = Files.createTempDirectory(prefix);
      // Ensure cleanup on JVM exit to prevent disk space issues.
      tempDir.toFile().deleteOnExit();
      return tempDir;
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to create temporary directory: " + prefix, e);
    }
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
  public static InputStream getResourceAsStream(@Nonnull final String name) {
    final ClassLoader loader = requireNonNull(Thread.currentThread().getContextClassLoader());
    final InputStream inputStream = loader.getResourceAsStream(name);
    requireNonNull(inputStream, "Test resource not found: " + name);
    return inputStream;
  }

  /**
   * Extracts a resource from the JAR to a temporary file on disk.
   *
   * <p>This is necessary because some operations (like Spark file reading) require actual file
   * paths rather than classpath resources. The method: 1. Creates a temporary file with a unique
   * name 2. Copies the resource content to the temporary file 3. Marks the file for deletion on JVM
   * exit
   *
   * @param resourceName The name/path of the resource to extract
   * @return A Path pointing to the temporary file containing the resource content
   * @throws RuntimeException if the extraction process fails
   */
  @Nonnull
  public static Path extractResourceToTempFile(@Nonnull final String resourceName) {
    try (final InputStream in = getResourceAsStream(resourceName)) {
      // Create a temporary file with a descriptive name for debugging.
      final Path tempFile =
          Files.createTempFile("pathling-benchmark-", "-" + resourceName.replace('/', '_'));
      // Ensure cleanup on JVM exit to prevent disk space accumulation.
      tempFile.toFile().deleteOnExit();

      // Copy resource content to the temporary file.
      try (final OutputStream out = Files.newOutputStream(tempFile)) {
        in.transferTo(out);
      }
      return tempFile;
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to extract resource: " + resourceName, e);
    }
  }
}
