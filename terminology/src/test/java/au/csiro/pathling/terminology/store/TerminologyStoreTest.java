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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trip tests for the terminology store: content written with Spark and Delta must read back
 * identically through Delta Kernel over the Hadoop FileSystem, the manifest must round-trip,
 * incompatible format versions and missing stores must be rejected, and an atomic per-version
 * replace must be visible without disturbing other versions.
 *
 * @author John Grimes
 */
class TerminologyStoreTest {

  private static final String COL_SYSTEM_VERSION_ID = "system_version_id";
  private static final String COL_CODE = "code";
  private static final String COL_DENSE_ID = "dense_id";
  private static final String COL_ACTIVE = "active";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("TerminologyStoreTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse.toString())
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  private static StructType conceptSchema() {
    return new StructType()
        .add(COL_SYSTEM_VERSION_ID, DataTypes.StringType, false)
        .add(COL_CODE, DataTypes.StringType, false)
        .add(COL_DENSE_ID, DataTypes.IntegerType, false)
        .add(COL_ACTIVE, DataTypes.BooleanType, false);
  }

  private Dataset<Row> conceptData(final List<Row> rows) {
    return spark.createDataFrame(rows, conceptSchema());
  }

  @Test
  void writesAndReadsManifest(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    final Instant importedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
    final ManifestEntry entry =
        new ManifestEntry(
            TerminologyStoreSchema.STORE_FORMAT_VERSION,
            "code_system",
            "http://snomed.info/sct",
            "http://snomed.info/sct/900000000000207008/version/20250101",
            "rf2.zip",
            importedAt);

    new TerminologyStoreWriter(spark, store).writeManifest(List.of(entry), SaveMode.Append);

    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final List<ManifestEntry> manifest = reader.readManifest();

    assertEquals(1, manifest.size());
    final ManifestEntry read = manifest.get(0);
    assertEquals(TerminologyStoreSchema.STORE_FORMAT_VERSION, read.getStoreFormatVersion());
    assertEquals("code_system", read.getEntryType());
    assertEquals("http://snomed.info/sct", read.getCanonicalUrl());
    assertEquals("http://snomed.info/sct/900000000000207008/version/20250101", read.getVersion());
    assertEquals("rf2.zip", read.getSource());
    assertEquals(importedAt, read.getImportedAt());
  }

  @Test
  void writesAndReadsTable(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, store);
    writer.writeManifest(minimalManifest(), SaveMode.Append);
    writer.writeTable(
        conceptData(
            List.of(RowFactory.create("v1", "A", 0, true), RowFactory.create("v1", "B", 1, false))),
        TerminologyStoreSchema.CONCEPT,
        SaveMode.Overwrite,
        List.of(COL_SYSTEM_VERSION_ID));

    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final List<String> rows = new ArrayList<>();
    reader.readTable(
        TerminologyStoreSchema.CONCEPT,
        row ->
            rows.add(
                row.getString(COL_SYSTEM_VERSION_ID)
                    + "|"
                    + row.getString(COL_CODE)
                    + "|"
                    + row.getInt(COL_DENSE_ID)
                    + "|"
                    + row.getBoolean(COL_ACTIVE)));

    assertEquals(new TreeSet<>(List.of("v1|A|0|true", "v1|B|1|false")), new TreeSet<>(rows));
  }

  @Test
  void rejectsNewerFormatVersion(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    final ManifestEntry future =
        new ManifestEntry(
            TerminologyStoreSchema.STORE_FORMAT_VERSION + 1,
            "code_system",
            "http://example.org/cs",
            "1.0.0",
            "future.json",
            Instant.now());
    new TerminologyStoreWriter(spark, store).writeManifest(List.of(future), SaveMode.Append);

    final TerminologyStoreException e =
        assertThrows(
            TerminologyStoreException.class, () -> TerminologyStoreReader.open(store, Map.of()));
    assertTrue(e.getMessage().contains("format version"), "message should mention format version");
  }

  @Test
  void rejectsMissingStore(@TempDir final Path storeDir) {
    final String missing = storeDir.resolve("does-not-exist").toString();
    assertThrows(
        TerminologyStoreException.class, () -> TerminologyStoreReader.open(missing, Map.of()));
  }

  @Test
  void atomicVersionReplaceIsVisible(@TempDir final Path storeDir) {
    final String store = storeDir.resolve("store").toString();
    final TerminologyStoreWriter writer = new TerminologyStoreWriter(spark, store);
    writer.writeManifest(minimalManifest(), SaveMode.Append);

    // Two versions coexist.
    writer.writeTable(
        conceptData(
            List.of(
                RowFactory.create("v1", "OLD", 0, true), RowFactory.create("v2", "KEEP", 0, true))),
        TerminologyStoreSchema.CONCEPT,
        SaveMode.Overwrite,
        List.of(COL_SYSTEM_VERSION_ID));

    // Replace only v1 with new content.
    writer.replaceWhere(
        conceptData(List.of(RowFactory.create("v1", "NEW", 0, true))),
        TerminologyStoreSchema.CONCEPT,
        "system_version_id = 'v1'");

    final TerminologyStoreReader reader = TerminologyStoreReader.open(store, Map.of());
    final List<String> rows = new ArrayList<>();
    reader.readTable(
        TerminologyStoreSchema.CONCEPT,
        row -> rows.add(row.getString(COL_SYSTEM_VERSION_ID) + "|" + row.getString(COL_CODE)));

    // v1's old content is gone and its new content is present; v2 is untouched.
    assertEquals(new TreeSet<>(List.of("v1|NEW", "v2|KEEP")), new TreeSet<>(rows));
  }

  private static List<ManifestEntry> minimalManifest() {
    return List.of(
        new ManifestEntry(
            TerminologyStoreSchema.STORE_FORMAT_VERSION,
            "code_system",
            "http://snomed.info/sct",
            "http://snomed.info/sct/900000000000207008/version/20250101",
            "rf2.zip",
            Instant.now()));
  }
}
