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

package au.csiro.pathling.terminology.store;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CANONICAL_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_RESOURCE_JSON;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The transient staging of the ValueSets of an import: an NDJSON file with one line per ValueSet,
 * holding the row the value set table stores for it, read back through Spark so the importer writes
 * the table once for the whole import without holding every ValueSet in driver memory. The file
 * lives in a driver-local temporary directory that is deleted when the staging is closed.
 *
 * @author John Grimes
 */
@Slf4j
public class ValueSetStaging implements AutoCloseable {

  private static final JsonFactory FACTORY = new JsonFactory();

  private static final String FILE_VALUE_SET = "value_set.ndjson";

  @Nonnull private final Path directory;
  @Nonnull private final JsonGenerator generator;
  private boolean sealed;

  private ValueSetStaging(@Nonnull final Path directory, @Nonnull final JsonGenerator generator) {
    this.directory = directory;
    this.generator = generator;
  }

  /**
   * Creates a fresh staging directory holding an empty file ready to append to.
   *
   * @return a new staging instance
   * @throws TerminologyImportException if the temporary directory or file cannot be created
   */
  @Nonnull
  public static ValueSetStaging create() {
    final Path directory;
    try {
      directory = SecureTempDirectory.create("pathling-value-set-");
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to create a temporary staging directory", e);
    }
    try {
      final JsonGenerator generator =
          FACTORY.createGenerator(
              Files.newOutputStream(directory.resolve(FILE_VALUE_SET)), JsonEncoding.UTF8);
      // Emit one JSON object per line so Spark reads the file as newline-delimited JSON.
      generator.setRootValueSeparator(new SerializedString("\n"));
      return new ValueSetStaging(directory, generator);
    } catch (final IOException e) {
      deleteDirectory(directory);
      throw new TerminologyImportException("Unable to create a temporary staging file", e);
    }
  }

  /**
   * Appends the row of a ValueSet.
   *
   * @param url the canonical URL
   * @param version the version, or null where the ValueSet has none
   * @param json the full R4 resource JSON
   */
  public void append(
      @Nonnull final String url, @Nullable final String version, @Nonnull final String json) {
    if (sealed) {
      throw new IllegalStateException("Staging has been sealed for reading and cannot be appended");
    }
    try {
      generator.writeStartObject();
      generator.writeStringField(COLUMN_CANONICAL_URL, url);
      generator.writeStringField(COLUMN_VERSION, version);
      generator.writeStringField(COLUMN_RESOURCE_JSON, json);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Flushes and closes the appender so the staging can be read back. No further rows may be
   * appended after sealing.
   */
  public void sealForReading() {
    if (sealed) {
      return;
    }
    sealed = true;
    try {
      generator.close();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads the sealed staging back as rows of the value set table. A ValueSet may be as large as the
   * whole-resource limit, far beyond the default limit on the length of a JSON string that Spark
   * reads, so that limit is lifted, and a row that still cannot be read fails the read rather than
   * being read as nulls.
   *
   * @param spark the Spark session to read with
   * @return the rows, lazily read from the staging file
   */
  @Nonnull
  public Dataset<Row> read(@Nonnull final SparkSession spark) {
    return spark
        .read()
        .schema(TerminologyStoreSchema.resourceTableSchema())
        .option("maxStringLen", Integer.MAX_VALUE)
        .option("mode", "FAILFAST")
        .json(directory.resolve(FILE_VALUE_SET).toUri().toString());
  }

  @Override
  public void close() {
    if (!sealed) {
      sealed = true;
      try {
        generator.close();
      } catch (final IOException e) {
        log.debug("Failed to close a staging appender during cleanup", e);
      }
    }
    deleteDirectory(directory);
  }

  private static void deleteDirectory(@Nonnull final Path directory) {
    try {
      Files.deleteIfExists(directory.resolve(FILE_VALUE_SET));
      Files.deleteIfExists(directory);
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary staging directory {}", directory, e);
    }
  }
}
