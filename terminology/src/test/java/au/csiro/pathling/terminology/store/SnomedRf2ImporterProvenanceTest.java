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
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.test.Rf2Mini;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies what an RF2 import records about the bytes it read: an archive source is fingerprinted
 * with the SHA-256 of the whole zip file, and a directory source records no hash at all. Each test
 * imports into a store of its own, since the source shape is what is under test.
 *
 * @author John Grimes
 */
class SnomedRf2ImporterProvenanceTest {

  private static SparkSession spark;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("SnomedRf2ImporterProvenanceTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", warehouse.toString())
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void zipSourceRecordsItsSha256(@TempDir final Path work) throws Exception {
    final Path archive = work.resolve("rf2.zip");
    zipDirectory(Rf2Mini.baseRelease(), archive);
    final String store = work.resolve("zip-store").toString();

    new SnomedRf2Importer(spark, store).importFrom(archive.toString(), null);

    final List<ManifestEntry> manifest = manifest(store);
    assertEquals(1, manifest.size());
    final ManifestEntry entry = manifest.get(0);
    // The recorded hash covers the whole archive, including the central directory that follows the
    // last entry the extraction read.
    assertEquals(sha256Hex(archive), entry.getSourceSha256());
    assertEquals(archive.toString(), entry.getSource());
    // An RF2 release is not a FHIR NPM package, so it carries no package identity or status.
    assertNull(entry.getPackageName());
    assertNull(entry.getPackageVersion());
    assertNull(entry.getPackageVerification());
    assertNull(entry.getPackageRegistry());
  }

  @Test
  void directorySourceRecordsNullHash(@TempDir final Path work) {
    final String store = work.resolve("dir-store").toString();

    new SnomedRf2Importer(spark, store).importFrom(Rf2Mini.baseRelease().toString(), null);

    final ManifestEntry entry = manifest(store).get(0);
    assertNull(entry.getSourceSha256());
    assertNull(entry.getPackageName());
    assertNull(entry.getPackageVersion());
    assertNull(entry.getPackageVerification());
    assertNull(entry.getPackageRegistry());
  }

  private static List<ManifestEntry> manifest(final String store) {
    return TerminologyStoreReader.open(store, Map.of()).readManifest();
  }

  /** Computes the SHA-256 of a file independently of the importer's own digesting stream. */
  private static String sha256Hex(final Path file) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream in = Files.newInputStream(file)) {
      final byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) != -1) {
        digest.update(buffer, 0, read);
      }
    }
    final StringBuilder builder = new StringBuilder();
    for (final byte b : digest.digest()) {
      builder.append(String.format("%02x", b));
    }
    return builder.toString();
  }

  /** Writes every regular file beneath {@code directory} into a zip archive at {@code archive}. */
  private static void zipDirectory(final Path directory, final Path archive) throws Exception {
    try (final OutputStream fileOut = Files.newOutputStream(archive);
        final ZipOutputStream zipOut = new ZipOutputStream(fileOut);
        final Stream<Path> files = Files.walk(directory)) {
      for (final Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
        zipOut.putNextEntry(new ZipEntry(directory.relativize(file).toString().replace('\\', '/')));
        Files.copy(file, zipOut);
        zipOut.closeEntry();
      }
    }
  }
}
