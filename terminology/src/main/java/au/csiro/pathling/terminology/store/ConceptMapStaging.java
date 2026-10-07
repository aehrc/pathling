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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EQUIVALENCE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ORDINAL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_SYSTEM;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;

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
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The transient staging of one ConceptMap streamed to disk: an NDJSON file with one line per
 * mapping, written by the flattener and read back through Spark so the driver never holds the
 * mappings of a map in memory. Each line refers to its group by index, and the systems of each
 * group, of which a map has few, are held here until the file is read back. The file lives in a
 * driver-local temporary directory that is deleted when the staging is closed.
 *
 * @author John Grimes
 */
@Slf4j
public class ConceptMapStaging implements AutoCloseable {

  private static final JsonFactory FACTORY = new JsonFactory();

  private static final String FILE_MAPPING = "mapping.ndjson";

  /** The staging column holding the index of the group a mapping belongs to. */
  private static final String COLUMN_GROUP = "group";

  /** The column naming the index of a group in the staged group systems. */
  private static final String COLUMN_GROUP_INDEX = "group_index";

  @Nonnull private final Path directory;
  @Nonnull private final JsonGenerator generator;
  @Nonnull private final List<Row> groups = new ArrayList<>();
  private int mappingCount;
  private boolean sealed;

  private ConceptMapStaging(@Nonnull final Path directory, @Nonnull final JsonGenerator generator) {
    this.directory = directory;
    this.generator = generator;
  }

  /**
   * Creates a fresh staging directory holding an empty mapping file ready to append to.
   *
   * @return a new staging instance
   * @throws TerminologyImportException if the temporary directory cannot be created
   */
  @Nonnull
  public static ConceptMapStaging create() {
    final Path directory;
    try {
      directory = SecureTempDirectory.create("pathling-concept-map-");
    } catch (final IOException e) {
      throw new TerminologyImportException("Unable to create a temporary staging directory", e);
    }
    try {
      final JsonGenerator generator =
          FACTORY.createGenerator(
              Files.newOutputStream(directory.resolve(FILE_MAPPING)), JsonEncoding.UTF8);
      // Emit one JSON object per line so Spark reads the file as newline-delimited JSON.
      generator.setRootValueSeparator(new SerializedString("\n"));
      return new ConceptMapStaging(directory, generator);
    } catch (final IOException e) {
      deleteDirectory(directory);
      throw new TerminologyImportException("Unable to create a temporary staging file", e);
    }
  }

  /**
   * Returns the number of groups registered so far, which is the index the next group will take.
   *
   * @return the number of registered groups
   */
  public int groupCount() {
    return groups.size();
  }

  /**
   * Registers the systems of the next group, in document order.
   *
   * @param sourceSystem the group's source system, or null where it names none
   * @param targetSystem the group's target system, or null where it names none
   */
  public void appendGroup(
      @Nullable final String sourceSystem, @Nullable final String targetSystem) {
    groups.add(RowFactory.create(groups.size(), sourceSystem, targetSystem));
  }

  /**
   * Appends a mapping row, numbering it with its position in document order.
   *
   * @param group the index of the group the mapping belongs to
   * @param sourceCode the code of the source concept
   * @param targetCode the code of the target concept, or null where the target carries none
   * @param equivalence the equivalence code of the mapping
   * @throws TerminologyImportException if the map holds more mappings than can be numbered
   */
  public void appendMapping(
      final int group,
      @Nonnull final String sourceCode,
      @Nullable final String targetCode,
      @Nonnull final String equivalence) {
    if (sealed) {
      throw new IllegalStateException("Staging has been sealed for reading and cannot be appended");
    }
    if (mappingCount == Integer.MAX_VALUE) {
      throw new TerminologyImportException(
          "A ConceptMap may hold at most " + Integer.MAX_VALUE + " mappings");
    }
    try {
      generator.writeStartObject();
      generator.writeNumberField(COLUMN_ORDINAL, mappingCount);
      generator.writeNumberField(COLUMN_GROUP, group);
      generator.writeStringField(COLUMN_SOURCE_CODE, sourceCode);
      generator.writeStringField(COLUMN_TARGET_CODE, targetCode);
      generator.writeStringField(COLUMN_EQUIVALENCE, equivalence);
      generator.writeEndObject();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    mappingCount++;
  }

  /**
   * Returns the number of mappings appended so far.
   *
   * @return the mapping count
   */
  public int mappingCount() {
    return mappingCount;
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
   * Reads the sealed staging back as mappings that carry their group's systems, in the columns
   * {@code ordinal}, {@code source_system}, {@code source_code}, {@code target_system}, {@code
   * target_code} and {@code equivalence}.
   *
   * @param spark the Spark session to read with
   * @return the mappings, lazily read from the staging file
   */
  @Nonnull
  public Dataset<Row> read(@Nonnull final SparkSession spark) {
    final StructType mappingSchema =
        new StructType()
            .add(COLUMN_ORDINAL, DataTypes.IntegerType, false)
            .add(COLUMN_GROUP, DataTypes.IntegerType, false)
            .add(COLUMN_SOURCE_CODE, DataTypes.StringType, false)
            .add(COLUMN_TARGET_CODE, DataTypes.StringType, true)
            .add(COLUMN_EQUIVALENCE, DataTypes.StringType, false);
    final StructType groupSchema =
        new StructType()
            .add(COLUMN_GROUP_INDEX, DataTypes.IntegerType, false)
            .add(COLUMN_SOURCE_SYSTEM, DataTypes.StringType, true)
            .add(COLUMN_TARGET_SYSTEM, DataTypes.StringType, true);
    final Dataset<Row> mappings =
        spark.read().schema(mappingSchema).json(directory.resolve(FILE_MAPPING).toUri().toString());
    final Dataset<Row> groupSystems = spark.createDataFrame(groups, groupSchema);
    return mappings
        .join(broadcast(groupSystems), col(COLUMN_GROUP).equalTo(col(COLUMN_GROUP_INDEX)))
        .select(
            col(COLUMN_ORDINAL),
            col(COLUMN_SOURCE_SYSTEM),
            col(COLUMN_SOURCE_CODE),
            col(COLUMN_TARGET_SYSTEM),
            col(COLUMN_TARGET_CODE),
            col(COLUMN_EQUIVALENCE));
  }

  @Override
  public void close() {
    if (!sealed) {
      sealed = true;
      try {
        generator.close();
      } catch (final IOException e) {
        log.debug("Failed to close the staging appender during cleanup", e);
      }
    }
    deleteDirectory(directory);
  }

  private static void deleteDirectory(@Nonnull final Path directory) {
    try {
      Files.deleteIfExists(directory.resolve(FILE_MAPPING));
      Files.deleteIfExists(directory);
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary staging directory {}", directory, e);
    }
  }
}
