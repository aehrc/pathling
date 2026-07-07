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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ENTRY_TYPE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_IMPORTED_AT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_STORE_FORMAT_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.STORE_FORMAT_VERSION;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;

/**
 * Reads a terminology store on executors through the Hadoop FileSystem API using Delta Kernel, with
 * no Spark dependency at query time. Each table read pins that table's latest Delta snapshot, so a
 * concurrent re-import that atomically replaces a version never exposes a partially written state
 * to an in-progress read.
 *
 * @author John Grimes
 */
public class TerminologyStoreReader {

  @Nonnull private final Engine engine;

  @Nonnull private final String storagePath;

  private TerminologyStoreReader(@Nonnull final Engine engine, @Nonnull final String storagePath) {
    this.engine = engine;
    this.storagePath = storagePath;
  }

  /**
   * Opens a terminology store, validating that it exists, has a readable manifest, and has a
   * compatible format version.
   *
   * @param storagePath the root path of the store
   * @param hadoopConfiguration a snapshot of the Hadoop configuration, used to reach the store's
   *     filesystem
   * @return an open reader
   * @throws TerminologyStoreException if the store is missing, unreadable, or of an incompatible
   *     format version
   */
  @Nonnull
  public static TerminologyStoreReader open(
      @Nonnull final String storagePath, @Nonnull final Map<String, String> hadoopConfiguration) {
    final Configuration configuration = new Configuration();
    hadoopConfiguration.forEach(configuration::set);
    final Engine engine = DefaultEngine.create(configuration);
    final TerminologyStoreReader reader = new TerminologyStoreReader(engine, storagePath);

    final List<ManifestEntry> manifest;
    try {
      manifest = reader.readManifest();
    } catch (final TerminologyStoreException e) {
      throw new TerminologyStoreException(
          "No readable terminology store at '"
              + storagePath
              + "'. The path may not exist or may not contain an imported store.",
          e);
    }

    final int maxFormat =
        manifest.stream()
            .mapToInt(ManifestEntry::getStoreFormatVersion)
            .max()
            .orElse(STORE_FORMAT_VERSION);
    if (maxFormat > STORE_FORMAT_VERSION) {
      throw new TerminologyStoreException(
          "Terminology store at '"
              + storagePath
              + "' has format version "
              + maxFormat
              + ", but this version of Pathling supports up to format version "
              + STORE_FORMAT_VERSION
              + ". Please upgrade Pathling to read this store.");
    }
    return reader;
  }

  /**
   * Reads the store manifest.
   *
   * @return the manifest entries
   * @throws TerminologyStoreException if the manifest cannot be read
   */
  @Nonnull
  public List<ManifestEntry> readManifest() {
    final List<ManifestEntry> entries = new ArrayList<>();
    readTable(
        TerminologyStoreSchema.MANIFEST,
        row ->
            entries.add(
                new ManifestEntry(
                    row.getInt(COLUMN_STORE_FORMAT_VERSION),
                    row.getString(COLUMN_ENTRY_TYPE),
                    row.getString(COLUMN_CANONICAL_URL),
                    row.getString(COLUMN_VERSION),
                    row.getString(COLUMN_SOURCE),
                    row.getInstant(COLUMN_IMPORTED_AT))));
    return entries;
  }

  /**
   * Streams the rows of a store table to a consumer. Each row is only valid for the duration of the
   * consumer call.
   *
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @param consumer receives each row in turn
   * @throws TerminologyStoreException if the table is missing or cannot be read
   */
  public void readTable(
      @Nonnull final String tableName, @Nonnull final Consumer<TerminologyStoreRow> consumer) {
    final String path = TerminologyStoreSchema.tablePath(storagePath, tableName);
    final Snapshot snapshot;
    try {
      snapshot = Table.forPath(engine, path).getLatestSnapshot(engine);
    } catch (final TableNotFoundException e) {
      throw new TerminologyStoreException(
          "Table '" + tableName + "' not found in terminology store: " + storagePath, e);
    }
    readSnapshot(tableName, snapshot, consumer);
  }

  /**
   * Streams the rows of an optional store table to a consumer, doing nothing if the table has never
   * been written. This supports tables that only some importers create (for example a FHIR-only
   * store has no reference set table), so an index over an absent table loads as empty rather than
   * failing.
   *
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @param consumer receives each row in turn
   * @throws TerminologyStoreException if the table exists but cannot be read
   */
  public void readTableIfPresent(
      @Nonnull final String tableName, @Nonnull final Consumer<TerminologyStoreRow> consumer) {
    final String path = TerminologyStoreSchema.tablePath(storagePath, tableName);
    final Snapshot snapshot;
    try {
      snapshot = Table.forPath(engine, path).getLatestSnapshot(engine);
    } catch (final TableNotFoundException e) {
      return;
    }
    readSnapshot(tableName, snapshot, consumer);
  }

  private void readSnapshot(
      @Nonnull final String tableName,
      @Nonnull final Snapshot snapshot,
      @Nonnull final Consumer<TerminologyStoreRow> consumer) {

    final Scan scan = snapshot.getScanBuilder().build();
    final Row scanState = scan.getScanState(engine);
    final StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);

    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine)) {
      while (scanFiles.hasNext()) {
        try (CloseableIterator<Row> scanFileRows = scanFiles.next().getRows()) {
          while (scanFileRows.hasNext()) {
            readDataFile(scanFileRows.next(), scanState, physicalReadSchema, consumer);
          }
        }
      }
    } catch (final IOException e) {
      throw new TerminologyStoreException(
          "Failed to read table '" + tableName + "' from terminology store: " + storagePath, e);
    }
  }

  private void readDataFile(
      @Nonnull final Row scanFileRow,
      @Nonnull final Row scanState,
      @Nonnull final StructType physicalReadSchema,
      @Nonnull final Consumer<TerminologyStoreRow> consumer)
      throws IOException {
    final FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
    final CloseableIterator<ColumnarBatch> physicalData =
        engine
            .getParquetHandler()
            .readParquetFiles(
                Utils.singletonCloseableIterator(fileStatus), physicalReadSchema, Optional.empty());
    try (CloseableIterator<FilteredColumnarBatch> transformed =
        Scan.transformPhysicalData(engine, scanState, scanFileRow, physicalData)) {
      while (transformed.hasNext()) {
        try (CloseableIterator<Row> dataRows = transformed.next().getRows()) {
          while (dataRows.hasNext()) {
            consumer.accept(new TerminologyStoreRow(dataRows.next()));
          }
        }
      }
    }
  }
}
