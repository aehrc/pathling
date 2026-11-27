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
package au.csiro.pathling.legacy;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Builds tables in the legacy layout, written by the legacy encoder exactly as the library wrote
 * them before the Parquet on FHIR layout, for the export to read.
 */
final class LegacyTables {

  /** The R4 context, built once because it is expensive to create. */
  @Nonnull static final FhirContext FHIR = FhirContext.forR4();

  /**
   * The encoders configured as the library configured them by default before the Parquet on FHIR
   * layout: three levels of nesting, extensions enabled and the standard open types.
   */
  @Nonnull
  static final FhirEncoders LIBRARY_DEFAULTS =
      FhirEncoders.forR4()
          .withMaxNestingLevel(3)
          .withExtensionsEnabled(true)
          .withOpenTypes(FhirEncoders.STANDARD_OPEN_TYPES)
          .getOrCreate();

  @Nullable private static SparkSession session;

  private LegacyTables() {}

  /**
   * Returns the Spark session shared by the tests, created on first use, with Delta Lake enabled.
   * It is not stopped between classes, because a Surefire fork runs several of them and a session
   * is expensive to build.
   *
   * @return the session
   */
  @Nonnull
  static synchronized SparkSession spark() {
    if (session == null) {
      session =
          SparkSession.builder()
              .master("local[2]")
              .appName("legacy-export-testing")
              .config("spark.driver.bindAddress", "localhost")
              .config("spark.driver.host", "localhost")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "1")
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config(
                  "spark.sql.catalog.spark_catalog",
                  "org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .getOrCreate();
    }
    return session;
  }

  /**
   * Encodes FHIR JSON documents with the legacy encoder.
   *
   * @param encoders the encoders to encode with
   * @param resourceType the type of every document
   * @param documents the documents
   * @return the encoded table
   */
  @Nonnull
  static Dataset<Row> encode(
      @Nonnull final FhirEncoders encoders,
      @Nonnull final String resourceType,
      @Nonnull final List<String> documents) {
    final IParser parser = FHIR.newJsonParser();
    final List<IBaseResource> resources =
        documents.stream().map(parser::parseResource).map(IBaseResource.class::cast).toList();
    return spark().createDataset(resources, encoders.<IBaseResource>of(resourceType)).toDF();
  }

  /**
   * Writes a table as Parquet.
   *
   * @param table the table
   * @param path where to write it
   * @return the path, as a string
   */
  @Nonnull
  static String writeParquet(@Nonnull final Dataset<Row> table, @Nonnull final Path path) {
    table.write().parquet(path.toString());
    return path.toString();
  }

  /**
   * Writes a table as Delta.
   *
   * @param table the table
   * @param path where to write it
   * @return the path, as a string
   */
  @Nonnull
  static String writeDelta(@Nonnull final Dataset<Row> table, @Nonnull final Path path) {
    table.write().format("delta").save(path.toString());
    return path.toString();
  }

  /**
   * Reads a file of newline-delimited JSON from the test resources.
   *
   * @param resource the path of the resource
   * @return its non-blank lines
   */
  @Nonnull
  static List<String> lines(@Nonnull final String resource) {
    try {
      final Path path =
          Path.of(Objects.requireNonNull(LegacyTables.class.getResource(resource)).toURI());
      try (final Stream<String> lines = Files.lines(path)) {
        return lines.filter(line -> !line.isBlank()).toList();
      }
    } catch (final URISyntaxException e) {
      throw new IllegalStateException("The resource is not readable: " + resource, e);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads every file of newline-delimited JSON that an export wrote for a resource type.
   *
   * @param output the directory the export wrote to
   * @param resourceType the resource type
   * @return the lines of every file, in no particular order
   */
  @Nonnull
  static List<String> exported(@Nonnull final Path output, @Nonnull final String resourceType) {
    try (final Stream<Path> files = Files.list(output)) {
      return files
          .filter(file -> isExportOf(file, resourceType))
          .flatMap(LegacyTables::readLines)
          .filter(line -> !line.isBlank())
          .toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Whether a file is one an export writes for a resource type, named {@code <type>.<part>.ndjson}.
   *
   * @param file the file
   * @param resourceType the resource type
   * @return whether it is
   */
  static boolean isExportOf(@Nonnull final Path file, @Nonnull final String resourceType) {
    final String name = file.getFileName().toString();
    return name.startsWith(resourceType + ".") && name.endsWith(".ndjson");
  }

  @Nonnull
  private static Stream<String> readLines(@Nonnull final Path file) {
    try {
      return Files.readAllLines(file).stream();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
