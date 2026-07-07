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

import jakarta.annotation.Nonnull;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
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
   * Writes the manifest of the store.
   *
   * @param entries the manifest entries to write
   * @param mode the save mode (typically {@link SaveMode#Append} to add new entries)
   */
  public void writeManifest(
      @Nonnull final List<ManifestEntry> entries, @Nonnull final SaveMode mode) {
    final List<Row> rows = new ArrayList<>(entries.size());
    for (final ManifestEntry entry : entries) {
      rows.add(
          RowFactory.create(
              entry.getStoreFormatVersion(),
              entry.getEntryType(),
              entry.getCanonicalUrl(),
              entry.getVersion(),
              entry.getSource(),
              entry.getImportedAt() == null ? null : Timestamp.from(entry.getImportedAt())));
    }
    final Dataset<Row> data = spark.createDataFrame(rows, TerminologyStoreSchema.manifestSchema());
    writeTable(data, TerminologyStoreSchema.MANIFEST, mode, List.of());
  }
}
