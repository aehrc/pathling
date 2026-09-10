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

package au.csiro.pathling.io;

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.encoders.FhirEncoders;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Reports, at startup, every Delta table in the warehouse whose schema differs from the current
 * FHIR encoders, in either direction, and migrates the migratable direction when {@code
 * schemaAutoMerge} is enabled.
 *
 * <p>A table missing fields the encoders require is migrated by an additive schema evolution.
 * Tables that remain in that state (flag disabled, or migration failure) are reported so that
 * requests against them can fail with an actionable error instead of a generic one.
 *
 * <p>A table carrying fields the encoders do not emit cannot be migrated - the columns cannot be
 * reconstructed - so it is reported and otherwise left alone. That condition is tolerated rather
 * than fatal, so it does not enter the drifted set.
 *
 * <p>Neither direction ever fails startup.
 *
 * @author John Grimes
 */
@Slf4j
public class SchemaMigrator {

  /** The file extension used for Delta table directories within the warehouse. */
  private static final String TABLE_EXTENSION = ".parquet";

  @Nonnull private final SparkSession spark;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final String databasePath;

  private final boolean schemaAutoMerge;

  /**
   * Constructs a new SchemaMigrator.
   *
   * @param spark the Spark session for Delta table operations
   * @param fhirEncoders the FHIR encoders whose schemas are the migration target
   * @param databasePath the path to the Delta database
   * @param schemaAutoMerge whether schema migration is enabled
   */
  public SchemaMigrator(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final String databasePath,
      final boolean schemaAutoMerge) {
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
    this.databasePath = databasePath;
    this.schemaAutoMerge = schemaAutoMerge;
  }

  /**
   * Compares every Delta table in the database path against the current encoder schema for its
   * resource type, migrating drifted tables when {@code schemaAutoMerge} is enabled. Migration is
   * additive only and never fails startup: per-table failures are logged and reported through the
   * returned set.
   *
   * @return the resource type codes that remain drifted and unmigrated
   */
  @Nonnull
  public Set<String> migrate() {
    final Set<String> driftedTypes = new HashSet<>();
    for (final String resourceCode : listTableResourceCodes()) {
      checkTable(resourceCode, driftedTypes);
    }
    return driftedTypes;
  }

  /**
   * Compares one table against its encoder schema and migrates it if drifted and permitted,
   * recording the type in the drifted set when it remains unmigrated. Never throws (FR-006).
   */
  private void checkTable(
      @Nonnull final String resourceCode, @Nonnull final Set<String> driftedTypes) {
    final String tablePath = safelyJoinPaths(databasePath, resourceCode + TABLE_EXTENSION);
    final SortedSet<String> missingFields;
    final SortedSet<String> excessFields;
    try {
      if (!DeltaTable.isDeltaTable(spark, tablePath)) {
        return;
      }
      final StructType encoderSchema = fhirEncoders.of(resourceCode).schema();
      final StructType tableSchema = spark.read().format("delta").load(tablePath).schema();
      missingFields = SchemaDrift.missingFieldPaths(encoderSchema, tableSchema);
      excessFields = SchemaDrift.excessFieldPaths(encoderSchema, tableSchema);
    } catch (final Exception e) {
      // A table that cannot be inspected (for example, an unencodable name) is skipped.
      log.debug("Skipping schema drift check for {}: {}", resourceCode, e.getMessage());
      return;
    }

    if (!excessFields.isEmpty()) {
      // This direction is not migratable: the columns cannot be reconstructed, and narrowing the
      // table would discard data. It is reported so that a deployment state which is otherwise
      // invisible - most often a narrowed pathling.encoding.openTypes against a populated warehouse
      // - can be seen before it has any effect, and at INFO because it is tolerated: writes
      // null-fill
      // these columns and reads return what the encoder can represent. The type is deliberately not
      // added to the drifted set, and no migration is attempted.
      log.info(
          "The {} table carries fields this server's encoders do not emit (extra fields: {}). These"
              + " columns are preserved and written as null; reads return what the encoders can"
              + " represent. To read them, restore the encoding configuration the table was written"
              + " with, in particular pathling.encoding.openTypes.",
          resourceCode,
          excessFields);
    }

    if (missingFields.isEmpty()) {
      return;
    }

    if (!schemaAutoMerge) {
      log.warn(
          "The {} table schema is behind this server's encoders (missing fields: {}). Requests "
              + "against this type will fail until it is migrated. Enable "
              + "pathling.storage.schemaAutoMerge and restart, or update a resource of this type "
              + "with the flag enabled, to migrate the table.",
          resourceCode,
          missingFields);
      driftedTypes.add(resourceCode);
      return;
    }

    try {
      // A zero-row append with mergeSchema adds the missing fields (including fields nested
      // inside structs, arrays and maps) to the table schema without touching any data; existing
      // rows present the new fields as null.
      QueryHelpers.createEmptyDataset(spark, fhirEncoders, resourceCode)
          .write()
          .format("delta")
          .mode(SaveMode.Append)
          .option("mergeSchema", "true")
          .save(tablePath);
      log.info("Migrated schema of {} table, added fields: {}", resourceCode, missingFields);
    } catch (final Exception e) {
      log.error(
          "Failed to migrate the schema of the {} table (missing fields: {}). Requests against "
              + "this type will fail until it is migrated.",
          resourceCode,
          missingFields,
          e);
      driftedTypes.add(resourceCode);
    }
  }

  /**
   * Lists the resource type codes for which a Delta table directory exists in the database path.
   * Non-directories and entries without the expected extension are skipped; a missing or empty
   * database path yields an empty list.
   */
  @Nonnull
  private List<String> listTableResourceCodes() {
    final List<String> resourceCodes = new ArrayList<>();
    try {
      final Path dbPath = new Path(databasePath);
      final FileSystem fileSystem =
          dbPath.getFileSystem(spark.sparkContext().hadoopConfiguration());
      if (!fileSystem.exists(dbPath)) {
        return resourceCodes;
      }
      for (final FileStatus status : fileSystem.listStatus(dbPath)) {
        final String name = status.getPath().getName();
        if (status.isDirectory() && name.endsWith(TABLE_EXTENSION)) {
          resourceCodes.add(name.substring(0, name.length() - TABLE_EXTENSION.length()));
        }
      }
    } catch (final IOException e) {
      log.warn("Unable to scan the warehouse for schema drift: {}", e.getMessage());
    }
    return resourceCodes;
  }
}
