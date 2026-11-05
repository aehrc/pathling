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

package au.csiro.pathling.library;

import static au.csiro.pathling.library.TerminologyHelpers.toCoding;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.sql.Terminology.translate;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.library.terminology.FhirImportOptions;
import au.csiro.pathling.terminology.local.LocalTerminologyServiceFactory;
import au.csiro.pathling.terminology.store.ManifestEntry;
import au.csiro.pathling.terminology.store.PackageVerification;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of FHIR terminology import through the public library API: the animal-species
 * fixtures are imported with {@link PathlingContext#importFhirTerminology}, a local-mode context is
 * created over the resulting store, and {@code member_of} and {@code translate} are evaluated as
 * UDFs (quickstart scenario 2 at the Java level).
 *
 * @author John Grimes
 */
class LocalTerminologyFhirImportTest {

  private static final String SYSTEM = "http://example.org/fhir/CodeSystem/animal-species";
  private static final String CATEGORY = "http://example.org/fhir/CodeSystem/animal-category";
  private static final String MAMMALS = "http://example.org/fhir/ValueSet/mammals-enumerated";
  private static final String CONCEPT_MAP =
      "http://example.org/fhir/ConceptMap/species-to-category";

  private static SparkSession spark;
  private static String store;

  @BeforeAll
  static void setUp(@TempDir final Path storeDir) {
    spark = TestHelpers.spark();
    store = storeDir.resolve("store").toString();
    PathlingContext.builder(spark).build().importFhirTerminology(fixturePath(), store);
  }

  @AfterAll
  static void tearDown() {
    LocalTerminologyServiceFactory.reset();
  }

  @BeforeEach
  void createLocalContext() {
    LocalTerminologyServiceFactory.reset();
    PathlingContext.builder(spark)
        .terminologyConfiguration(
            TerminologyConfiguration.builder()
                .mode(TerminologyMode.LOCAL)
                .local(LocalTerminologyConfiguration.builder().storagePath(store).build())
                .build())
        .build();
  }

  private static String fixturePath() {
    return Path.of("..", "terminology", "src", "test", "resources", "fhir-fixtures", "json")
        .toAbsolutePath()
        .normalize()
        .toString();
  }

  private static Column species(final String code) {
    return toCoding(lit(code), SYSTEM, null);
  }

  private static Row evaluate(final Column result) {
    final Dataset<Row> df =
        spark.createDataFrame(
            List.of(RowFactory.create("row")),
            new StructType().add("id", DataTypes.StringType, true));
    return df.select(result.alias("result")).collectAsList().get(0);
  }

  @Test
  void memberOfExplicitValueSet() {
    assertTrue(evaluate(member_of(species("dog"), MAMMALS)).getBoolean(0));
    assertFalse(evaluate(member_of(species("sparrow"), MAMMALS)).getBoolean(0));
  }

  @Test
  void translateThroughImportedConceptMap() {
    // Dog maps to a single category concept through the imported concept map.
    final List<Row> matches =
        evaluate(translate(species("dog"), CONCEPT_MAP, false, null)).getList(0);
    assertEquals(1, matches.size());
    assertEquals(CATEGORY, matches.get(0).getString(matches.get(0).fieldIndex("system")));
  }

  @Test
  void optionsOverloadPassesVerifyPackageFalse(@TempDir final Path dir) throws Exception {
    final Path archive = buildPackage(dir);
    final String packageStore = dir.resolve("package-store").toString();

    PathlingContext.builder(spark)
        .build()
        .importFhirTerminology(
            archive.toString(),
            packageStore,
            FhirImportOptions.builder().verifyPackage(false).build());

    final List<ManifestEntry> manifest =
        TerminologyStoreReader.open(packageStore, Map.of()).readManifest();
    assertFalse(manifest.isEmpty());
    for (final ManifestEntry entry : manifest) {
      // The caller declined the check, so no registry was consulted and the store says so.
      assertEquals(PackageVerification.SKIPPED, entry.getPackageVerification());
      assertNull(entry.getPackageRegistry());
    }
  }

  @Test
  void nullOptionsAreAccepted(@TempDir final Path dir) {
    final String directoryStore = dir.resolve("directory-store").toString();

    PathlingContext.builder(spark)
        .build()
        .importFhirTerminology(fixturePath(), directoryStore, null);

    final List<ManifestEntry> manifest =
        TerminologyStoreReader.open(directoryStore, Map.of()).readManifest();
    assertFalse(manifest.isEmpty());
    // A directory source has no package identity and no verification outcome at all.
    assertNull(manifest.get(0).getPackageVerification());
    assertNull(manifest.get(0).getSourceSha256());
  }

  /** Builds a FHIR NPM package from the fixture resources, so no network or registry is needed. */
  private static Path buildPackage(final Path directory) throws Exception {
    final Path archive = directory.resolve("fixtures.tgz");
    try (TarArchiveOutputStream tar =
        new TarArchiveOutputStream(
            new GzipCompressorOutputStream(Files.newOutputStream(archive)))) {
      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      writeEntry(
          tar,
          "package/package.json",
          "{\"name\":\"example.fhir.animals\",\"version\":\"1.0.0\"}"
              .getBytes(StandardCharsets.UTF_8));
      try (Stream<Path> files = Files.list(Path.of(fixturePath()))) {
        for (final Path file : files.filter(p -> p.toString().endsWith(".json")).toList()) {
          writeEntry(tar, "package/" + file.getFileName(), Files.readAllBytes(file));
        }
      }
    }
    return archive;
  }

  private static void writeEntry(
      final TarArchiveOutputStream tar, final String name, final byte[] content) throws Exception {
    final TarArchiveEntry entry = new TarArchiveEntry(name);
    entry.setSize(content.length);
    tar.putArchiveEntry(entry);
    final OutputStream out = tar;
    out.write(content);
    tar.closeArchiveEntry();
  }
}
