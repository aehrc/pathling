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
import static org.apache.spark.sql.functions.col;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Option;

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

  /** The Spark SQL configuration key that lets adaptive execution coalesce shuffle partitions. */
  private static final String COALESCE_PARTITIONS_KEY =
      "spark.sql.adaptive.coalescePartitions.enabled";

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
   * Writes a partitioned content table for several partition values in one commit, replacing the
   * partitions of those values atomically or creating the table on first write.
   *
   * <p>The rows are rebalanced by the partition column, so each partition value is written by one
   * task, or by several where adaptive execution splits a large one, rather than by every task that
   * holds some of its rows. Without this, a source of many small resources leaves a file per
   * resource per task. Adaptive execution would otherwise coalesce a small rebalance into a single
   * task, which then writes every partition in turn; the cost of a partition is dominated by the
   * file it writes rather than its rows, so coalescing is disabled for the duration of the write.
   *
   * @param data the rows to write, each carrying one of {@code values} in {@code partitionColumn}
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @param partitionColumn the column the table is partitioned by, such as {@code
   *     system_version_id}
   * @param values the partition values the rows belong to, not empty
   */
  public void writePartitions(
      @Nonnull final Dataset<Row> data,
      @Nonnull final String tableName,
      @Nonnull final String partitionColumn,
      @Nonnull final Collection<String> values) {
    final Dataset<Row> rebalanced = data.hint("rebalance", col(partitionColumn));
    final RuntimeConfig conf = spark.conf();
    final Option<String> previous = conf.getOption(COALESCE_PARTITIONS_KEY);
    conf.set(COALESCE_PARTITIONS_KEY, false);
    try {
      if (tableExists(tableName)) {
        replaceWhere(rebalanced, tableName, partitionColumn + " IN (" + sqlStrings(values) + ")");
      } else {
        writeTable(rebalanced, tableName, SaveMode.Overwrite, List.of(partitionColumn));
      }
    } finally {
      if (previous.isDefined()) {
        conf.set(COALESCE_PARTITIONS_KEY, previous.get());
      } else {
        conf.unset(COALESCE_PARTITIONS_KEY);
      }
    }
  }

  /**
   * Writes a table keyed by canonical URL and version, replacing the rows of the given keys
   * atomically in one commit or creating the table on first write.
   *
   * @param data the rows to write, each matching one of {@code keys}
   * @param tableName the table name (a {@link TerminologyStoreSchema} constant)
   * @param keys the canonical URL and version of every resource being written, not empty
   */
  public void writeByCanonicalVersion(
      @Nonnull final Dataset<Row> data,
      @Nonnull final String tableName,
      @Nonnull final Collection<CanonicalVersion> keys) {
    if (tableExists(tableName)) {
      replaceWhere(data, tableName, canonicalVersionPredicate(keys));
    } else {
      writeTable(data, tableName, SaveMode.Overwrite, List.of());
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
    writeManifestRows(data, mode, null);
  }

  /**
   * Adds or replaces a single manifest entry, keyed by canonical URL and version, atomically. The
   * entry is appended when the manifest does not yet exist and replaces the matching entry
   * otherwise, leaving other entries untouched.
   *
   * @param entry the manifest entry to upsert
   */
  public void upsertManifestEntry(@Nonnull final ManifestEntry entry) {
    upsertManifestEntries(List.of(entry));
  }

  /**
   * Adds or replaces manifest entries, keyed by canonical URL and version, atomically in one
   * commit. The entries are appended when the manifest does not yet exist and replace the matching
   * entries otherwise, leaving other entries untouched.
   *
   * @param entries the manifest entries to upsert, not empty
   */
  public void upsertManifestEntries(@Nonnull final List<ManifestEntry> entries) {
    if (tableExists(MANIFEST)) {
      final Dataset<Row> data =
          spark.createDataFrame(
              entries.stream().map(TerminologyStoreWriter::manifestRow).toList(),
              TerminologyStoreSchema.manifestSchema());
      writeManifestRows(
          data,
          SaveMode.Overwrite,
          canonicalVersionPredicate(
              entries.stream()
                  .map(entry -> new CanonicalVersion(entry.getCanonicalUrl(), entry.getVersion()))
                  .toList()));
    } else {
      writeManifest(entries, SaveMode.Append);
    }
  }

  /**
   * Writes rows into the manifest, allowing the manifest schema to gain columns that a store
   * written by an earlier version of Pathling does not have. Schema drift on content tables remains
   * a failure.
   */
  private void writeManifestRows(
      @Nonnull final Dataset<Row> data,
      @Nonnull final SaveMode mode,
      @Nullable final String condition) {
    var writer = data.write().format(DELTA_FORMAT).mode(mode).option("mergeSchema", "true");
    if (condition != null) {
      writer = writer.option("replaceWhere", condition);
    }
    writer.save(TerminologyStoreSchema.tablePath(storagePath, MANIFEST));
  }

  @Nonnull
  private static Row manifestRow(@Nonnull final ManifestEntry entry) {
    final PackageVerification verification = entry.getPackageVerification();
    return RowFactory.create(
        entry.getStoreFormatVersion(),
        entry.getEntryType(),
        entry.getCanonicalUrl(),
        entry.getVersion(),
        entry.getSource(),
        entry.getImportedAt() == null ? null : Timestamp.from(entry.getImportedAt()),
        entry.getSourceSha256(),
        entry.getPackageName(),
        entry.getPackageVersion(),
        verification == null ? null : verification.getCode(),
        entry.getPackageRegistry(),
        entry.getFault());
  }

  /** Builds a SQL predicate matching a nullable version. */
  @Nonnull
  private static String versionPredicate(@Nullable final String version) {
    return version == null
        ? COLUMN_VERSION + " IS NULL"
        : COLUMN_VERSION + " = " + sqlString(version);
  }

  /**
   * Builds a SQL predicate matching any of a set of canonical URL and version pairs. The pairs are
   * grouped by version, so the predicate has one term per distinct version rather than one per
   * pair.
   *
   * @param keys the pairs to match, not empty
   * @return the predicate
   */
  @Nonnull
  static String canonicalVersionPredicate(@Nonnull final Collection<CanonicalVersion> keys) {
    final Map<String, List<String>> urlsByVersion = new LinkedHashMap<>();
    for (final CanonicalVersion key : keys) {
      urlsByVersion.computeIfAbsent(key.version(), version -> new ArrayList<>()).add(key.url());
    }
    return urlsByVersion.entrySet().stream()
        .map(
            byVersion ->
                "("
                    + versionPredicate(byVersion.getKey())
                    + " AND "
                    + COLUMN_CANONICAL_URL
                    + " IN ("
                    + sqlStrings(byVersion.getValue())
                    + "))")
        .collect(Collectors.joining(" OR "));
  }

  /** Renders values as a comma-separated list of SQL string literals. */
  @Nonnull
  private static String sqlStrings(@Nonnull final Collection<String> values) {
    return values.stream()
        .distinct()
        .map(TerminologyStoreWriter::sqlString)
        .collect(Collectors.joining(", "));
  }

  /**
   * Renders a value as a SQL string literal, escaping the backslashes and quotes that would
   * otherwise end the literal early or change its meaning.
   */
  @Nonnull
  private static String sqlString(@Nonnull final String value) {
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  /**
   * The canonical URL and version that key a resource in the manifest and the value set table.
   *
   * @param url the canonical URL
   * @param version the version, or null where the resource has none
   */
  public record CanonicalVersion(@Nonnull String url, @Nullable String version) {}
}
