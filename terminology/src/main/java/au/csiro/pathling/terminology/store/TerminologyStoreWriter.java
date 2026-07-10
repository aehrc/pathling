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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.MANIFEST;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Writes terminology store tables using Spark and Delta Lake. Import code uses this to persist the
 * store; each table is written as a Delta table under the store root. Per-version content is
 * replaced atomically through Delta's {@code replaceWhere}, so concurrent readers pinned to an
 * earlier snapshot always see a consistent version.
 *
 * @author John Grimes
 */
public class TerminologyStoreWriter {

  private static final String DELTA_FORMAT = "delta";

  @Nonnull private final SparkSession spark;

  @Nonnull private final String storagePath;

  /**
   * Creates a writer for a store.
   *
   * @param spark the Spark session used to write
   * @param storagePath the root path of the store
   */
  public TerminologyStoreWriter(
      @Nonnull final SparkSession spark, @Nonnull final String storagePath) {
    this.spark = spark;
    this.storagePath = storagePath;
  }

  /**
   * Writes a table to the store.
   *
   * @param data the rows to write
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @param mode the save mode (typically {@link SaveMode#Overwrite} or {@link SaveMode#Append})
   * @param partitionColumns the columns to partition by, empty for none
   */
  public void writeTable(
      @Nonnull final Dataset<Row> data,
      @Nonnull final String tableName,
      @Nonnull final SaveMode mode,
      @Nonnull final List<String> partitionColumns) {
    var writer = data.write().format(DELTA_FORMAT).mode(mode);
    if (!partitionColumns.isEmpty()) {
      writer = writer.partitionBy(partitionColumns.toArray(new String[0]));
    }
    writer.save(TerminologyStoreSchema.tablePath(storagePath, tableName));
  }

  /**
   * Atomically replaces the rows of a table that match a condition, leaving all other rows
   * untouched. The supplied data must satisfy the condition. This is the mechanism for re-importing
   * a single code system version without disturbing other versions.
   *
   * @param data the replacement rows, all of which must match {@code condition}
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @param condition a SQL predicate selecting the rows to replace (for example {@code
   *     "system_version_id = 'abc'"})
   */
  public void replaceWhere(
      @Nonnull final Dataset<Row> data,
      @Nonnull final String tableName,
      @Nonnull final String condition) {
    data.write()
        .format(DELTA_FORMAT)
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", condition)
        .save(TerminologyStoreSchema.tablePath(storagePath, tableName));
  }

  /**
   * Writes a content table partitioned by {@code system_version_id}, replacing an existing
   * version's partition atomically or creating the table on first write. All rows must belong to
   * the given system version.
   *
   * @param data the rows to write, all belonging to {@code systemVersionId}
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @param systemVersionId the code system version identifier the rows belong to
   */
  public void writePartitionedBySystemVersion(
      @Nonnull final Dataset<Row> data,
      @Nonnull final String tableName,
      @Nonnull final String systemVersionId) {
    if (tableExists(tableName)) {
      replaceWhere(data, tableName, COLUMN_SYSTEM_VERSION_ID + " = '" + systemVersionId + "'");
    } else {
      writeTable(data, tableName, SaveMode.Overwrite, List.of(COLUMN_SYSTEM_VERSION_ID));
    }
  }

  /**
   * Returns whether a table already exists in the store (has a Delta transaction log).
   *
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @return true if the table exists
   */
  public boolean tableExists(@Nonnull final String tableName) {
    final Configuration hadoopConf = spark.sessionState().newHadoopConf();
    final Path log =
        new Path(TerminologyStoreSchema.tablePath(storagePath, tableName), "_delta_log");
    try {
      return log.getFileSystem(hadoopConf).exists(log);
    } catch (final IOException e) {
      return false;
    }
  }

  /**
   * Writes the manifest of the store.
   *
   * @param entries the manifest entries to write
   * @param mode the save mode (typically {@link SaveMode#Append} to add new entries)
   */
  public void writeManifest(
      @Nonnull final List<ManifestEntry> entries, @Nonnull final SaveMode mode) {
    final List<Row> rows = new ArrayList<>(entries.size());
    for (final ManifestEntry entry : entries) {
      rows.add(manifestRow(entry));
    }
    final Dataset<Row> data = spark.createDataFrame(rows, TerminologyStoreSchema.manifestSchema());
    writeTable(data, MANIFEST, mode, List.of());
  }

  /**
   * Adds or replaces a single manifest entry, keyed by canonical URL and version, atomically. The
   * entry is appended when the manifest does not yet exist and replaces the matching entry
   * otherwise, leaving other entries untouched.
   *
   * @param entry the manifest entry to upsert
   */
  public void upsertManifestEntry(@Nonnull final ManifestEntry entry) {
    if (tableExists(MANIFEST)) {
      final Dataset<Row> data =
          spark.createDataFrame(
              List.of(manifestRow(entry)), TerminologyStoreSchema.manifestSchema());
      replaceWhere(
          data,
          MANIFEST,
          COLUMN_CANONICAL_URL
              + " = '"
              + entry.getCanonicalUrl()
              + "' AND "
              + versionPredicate(entry.getVersion()));
    } else {
      writeManifest(List.of(entry), SaveMode.Append);
    }
  }

  @Nonnull
  private static Row manifestRow(@Nonnull final ManifestEntry entry) {
    return RowFactory.create(
        entry.getStoreFormatVersion(),
        entry.getEntryType(),
        entry.getCanonicalUrl(),
        entry.getVersion(),
        entry.getSource(),
        entry.getImportedAt() == null ? null : Timestamp.from(entry.getImportedAt()));
  }

  /** Builds a SQL predicate matching a nullable version. */
  @Nonnull
  static String versionPredicate(@Nullable final String version) {
    return version == null ? COLUMN_VERSION + " IS NULL" : COLUMN_VERSION + " = '" + version + "'";
  }
}
