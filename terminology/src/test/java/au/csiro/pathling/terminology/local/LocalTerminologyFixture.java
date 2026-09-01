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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.terminology.local.index.CodeSystemIndexes;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * Imports the rf2-mini fixture into a temporary store exactly once for the whole test JVM, so the
 * several local-terminology test classes share a single expensive import. The store is read through
 * Delta Kernel, which has no Spark dependency, so the reader remains usable after the import even
 * if another test stops the shared Spark session.
 *
 * @author John Grimes
 */
final class LocalTerminologyFixture {

  private static String storagePath;
  private static TerminologyStoreReader reader;
  private static String systemVersionId;

  private LocalTerminologyFixture() {
    // Static holder.
  }

  static synchronized void ensure() {
    if (reader != null) {
      return;
    }
    final SparkSession spark =
        SparkSession.builder()
            .appName("LocalTerminologyFixture")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    try {
      storagePath = Files.createTempDirectory("rf2-mini-store").resolve("store").toString();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    new SnomedRf2Importer(spark, storagePath).importFrom(Rf2Mini.baseRelease().toString(), null);
    reader = TerminologyStoreReader.open(storagePath, Map.of());
    systemVersionId =
        CodeSystemEntry.loadCatalogue(reader).stream()
            .filter(entry -> Rf2Mini.VERSION_20230601.equals(entry.getVersion()))
            .map(CodeSystemEntry::getSystemVersionId)
            .findFirst()
            .orElseThrow();
  }

  @Nonnull
  static String storagePath() {
    ensure();
    return storagePath;
  }

  @Nonnull
  static TerminologyStoreReader reader() {
    ensure();
    return reader;
  }

  @Nonnull
  static String systemVersionId() {
    ensure();
    return systemVersionId;
  }

  @Nonnull
  static CodeSystemIndexes indexes() {
    ensure();
    return CodeSystemIndexes.load(reader, systemVersionId);
  }

  @Nonnull
  static List<CodeSystemEntry> catalogue() {
    ensure();
    return CodeSystemEntry.loadCatalogue(reader);
  }
}
