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

import au.csiro.pathling.test.Rf2Mini;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that an RF2 release whose relationship or reference set files contribute no active rows
 * still imports. Extension releases routinely ship snapshot files that hold only a header. The row
 * count of such a file is observed through a query whose metrics Spark may report with no schema at
 * all, and the import must complete and write its manifest regardless.
 *
 * @author John Grimes
 */
class SnomedRf2ImporterEmptyFileTest {

  private static SparkSession spark;

  @BeforeAll
  static void setUp(@TempDir final Path warehouse) {
    spark =
        SparkSession.builder()
            .appName("SnomedRf2ImporterEmptyFileTest")
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
  void importsAReleaseWithHeaderOnlyRelationshipAndRefsetFiles(@TempDir final Path work)
      throws IOException {
    // Copy the base release and reduce its relationship file and every reference set file other
    // than the language reference set to their header lines.
    final Path release = copyRelease(work.resolve("release"));
    truncateToHeader(release, "sct2_Relationship_Snapshot");
    truncateToHeader(release, "der2_Refset_SimpleSnapshot");
    truncateToHeader(release, "der2_cRefset_AssociationSnapshot");
    final String store = work.resolve("store").toString();

    new SnomedRf2Importer(spark, store).importFrom(release.toString(), null);

    // The import completes, so its manifest is written.
    assertEquals(1, manifest(store).size());
  }

  @Test
  void importsAReleaseWhoseRefsetRowsAreAllInactive(@TempDir final Path work) throws IOException {
    // Copy the base release and mark every row of the simple reference set as inactive, so the
    // file has rows but contributes none.
    final Path release = copyRelease(work.resolve("release"));
    deactivateAllRows(release, "der2_Refset_SimpleSnapshot");
    final String store = work.resolve("store").toString();

    new SnomedRf2Importer(spark, store).importFrom(release.toString(), null);

    assertEquals(1, manifest(store).size());
  }

  private static List<ManifestEntry> manifest(final String store) {
    return TerminologyStoreReader.open(store, Map.of()).readManifest();
  }

  /** Copies the base RF2 release fixture into a fresh directory. */
  private static Path copyRelease(final Path target) throws IOException {
    final Path source = Rf2Mini.baseRelease();
    try (Stream<Path> files = Files.walk(source)) {
      for (final Path file : (Iterable<Path>) files::iterator) {
        final Path copy = target.resolve(source.relativize(file).toString());
        if (Files.isDirectory(file)) {
          Files.createDirectories(copy);
        } else {
          Files.copy(file, copy);
        }
      }
    }
    return target;
  }

  /** Finds the single file in a release whose name starts with the given prefix. */
  private static Path findFile(final Path release, final String prefix) throws IOException {
    try (Stream<Path> files = Files.walk(release)) {
      return files
          .filter(p -> p.getFileName().toString().startsWith(prefix))
          .findFirst()
          .orElseThrow();
    }
  }

  /** Reduces an RF2 file to its header line. */
  private static void truncateToHeader(final Path release, final String prefix) throws IOException {
    final Path file = findFile(release, prefix);
    Files.writeString(file, Files.readAllLines(file).get(0) + "\n");
  }

  /** Sets the active column, the third, of every data row of an RF2 file to zero. */
  private static void deactivateAllRows(final Path release, final String prefix)
      throws IOException {
    final Path file = findFile(release, prefix);
    final List<String> lines = Files.readAllLines(file);
    final StringBuilder content = new StringBuilder(lines.get(0)).append('\n');
    for (final String line : lines.subList(1, lines.size())) {
      final String[] fields = line.split("\t", -1);
      fields[2] = "0";
      content.append(String.join("\t", fields)).append('\n');
    }
    Files.writeString(file, content.toString());
  }
}
