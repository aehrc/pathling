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
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.DatasetSource;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Migrates the warehouse, at startup, from the layout written before Parquet on FHIR (the legacy
 * layout) to the Parquet on FHIR layout, which is the only layout the library reads.
 *
 * <p>Each resource table in the legacy layout is converted by {@link LegacyLayoutConversion} and
 * overwritten in place. The overwrite is a new Delta version, so the data as it was before the
 * migration stays readable through time travel until the table is vacuumed. Tables already in the
 * Parquet on FHIR layout, or in no recognisable layout, are not written.
 *
 * <p>Each table is migrated by a single atomic overwrite, so a migration that stops part of the way
 * through leaves every table in one layout or the other, and running it again continues with the
 * tables still in the legacy layout. A table that cannot be migrated fails the migration, because
 * the server cannot serve it.
 *
 * @author John Grimes
 */
@Slf4j
public class WarehouseMigrator {

  /** The directory-name suffix under which each resource type's Delta table is stored. */
  private static final String TABLE_SUFFIX = ".parquet";

  /** The column holding the logical id of each resource. */
  private static final String ID = "id";

  @Nonnull private final PathlingContext context;

  @Nonnull private final SparkSession spark;

  @Nonnull private final String databasePath;

  /**
   * Constructs a new WarehouseMigrator.
   *
   * @param context the Pathling context, used to convert and write the tables
   * @param databasePath the path to the Delta database holding one table per resource type
   */
  public WarehouseMigrator(
      @Nonnull final PathlingContext context, @Nonnull final String databasePath) {
    this.context = context;
    this.spark = context.getSpark();
    this.databasePath = databasePath;
  }

  /**
   * Migrates every resource table in the database that is in the legacy layout.
   *
   * @throws IllegalStateException if a table cannot be migrated, naming the table and the cause
   * @throws UncheckedIOException if the database directory cannot be listed
   */
  public void migrate() {
    listResourceTypes().forEach(this::migrateTable);
  }

  /**
   * Converts and overwrites the table of one resource type if it is a Delta table in the legacy
   * layout.
   */
  private void migrateTable(@Nonnull final String resourceType) {
    final String tablePath = safelyJoinPaths(databasePath, resourceType + TABLE_SUFFIX);
    try {
      if (!DeltaTable.isDeltaTable(spark, tablePath)) {
        log.debug("Skipping {}, which is not a Delta table", tablePath);
        return;
      }
      final DeltaTable table = DeltaTable.forPath(spark, tablePath);
      final Dataset<Row> data = table.toDF();
      if (!LegacyLayoutConversion.isLegacy(data.schema())) {
        return;
      }

      final long previousVersion = table.history(1).select("version").first().getLong(0);
      log.info(
          "Migrating the {} table at {} to the Parquet on FHIR layout", resourceType, tablePath);
      final Dataset<Row> converted =
          withIdColumn(LegacyLayoutConversion.convertIfLegacy(context, resourceType, data));
      // The library's sink applies its own checks to the write. An overwrite replaces the schema
      // along with the data, and the files of the previous version stay in place for time travel.
      new DataSinkBuilder(context, new DatasetSource(context).dataset(resourceType, converted))
          .saveMode("overwrite")
          .delta(databasePath);

      log.warn(
          "The {} table at {} has been migrated from the layout written by earlier releases of"
              + " Pathling to the Parquet on FHIR layout. In the migrated data, {}. The data as it"
              + " was before the migration remains readable at Delta version {} of the table (read"
              + " it with the versionAsOf option) until the table is vacuumed.",
          resourceType,
          tablePath,
          LegacyLayoutConversion.LOSSES,
          previousVersion);
      log.info(
          "Migrated the {} table, which now holds {} resources",
          resourceType,
          spark.read().format("delta").load(tablePath).count());
    } catch (final RuntimeException e) {
      throw new IllegalStateException(
          "Failed to migrate the "
              + resourceType
              + " table at "
              + tablePath
              + " to the Parquet on FHIR layout: "
              + e.getMessage(),
          e);
    }
  }

  /**
   * Adds an id column to converted data that lacks one. The layout fits its schema to the data, so
   * an empty table converts to one without an id column, which the server selects and filters on
   * for every type - as {@link ResourceEncoding#empty} provides for a type with no table.
   */
  @Nonnull
  private static Dataset<Row> withIdColumn(@Nonnull final Dataset<Row> converted) {
    return Arrays.asList(converted.columns()).contains(ID)
        ? converted
        : converted.withColumn(ID, lit(null).cast(DataTypes.StringType));
  }

  /**
   * Lists the resource types with a table directory in the database directory. Entries that are not
   * directories, lack the table suffix, or do not name a supported resource type are skipped; a
   * missing database directory yields no types.
   */
  @Nonnull
  private List<String> listResourceTypes() {
    final Path databaseDir = new Path(databasePath);
    try {
      final FileSystem fileSystem =
          databaseDir.getFileSystem(spark.sparkContext().hadoopConfiguration());
      if (!fileSystem.exists(databaseDir)) {
        return List.of();
      }
      return Arrays.stream(fileSystem.listStatus(databaseDir))
          .filter(FileStatus::isDirectory)
          .map(status -> status.getPath().getName())
          .filter(name -> name.endsWith(TABLE_SUFFIX))
          .map(name -> name.substring(0, name.length() - TABLE_SUFFIX.length()))
          .filter(context::isResourceTypeSupported)
          .sorted()
          .toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(
          "Failed to list the database directory " + databasePath + " for migration", e);
    }
  }
}
