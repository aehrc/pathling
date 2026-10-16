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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.test.NoNetworkExtension;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests local {@code translate} through SNOMED CT simple map reference sets, which THO defines as
 * implicit concept maps alongside the association reference sets: every reference set that descends
 * from {@code 900000000000496009 |Simple map type reference set|}. The simple maps Ontoserver
 * supports are also accepted wherever they sit, because current releases have moved CTV3 under
 * {@code 1187636009 |Simple map to SNOMED CT type reference set|}, and they carry the target system
 * and equivalence Ontoserver gives them.
 *
 * <p>The shared rf2-mini fixture holds no simple map, and adding one would change the concept
 * counts that many tests across the modules depend upon, so this class writes a purpose-built
 * release instead. Its hierarchy is:
 *
 * <pre>
 * 138875005 (root)
 * ├── 900000000000496009 (simple map type)
 * │   ├── 446608001 (ICD-O), 11000168105 (ARTG)
 * │   ├── 467614008 (GMDN, with no known target system)
 * │   └── 447562003 (ICD-10 extended map, placed here only for this test)
 * ├── 1187636009 (simple map to SNOMED CT type)
 * │   └── 900000000000497000 (CTV3), 1193497006 (MedDRA to SNOMED CT)
 * └── FINDING
 *     └── SOURCE_A .. SOURCE_E
 * </pre>
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceSimpleMapTest {

  private static final String SNOMED = "http://snomed.info/sct";
  private static final String MODULE = "900000000000207008";
  private static final String EFFECTIVE_TIME = "20240101";
  private static final String IS_A = "116680003";

  // Real metadata identifiers.
  private static final String ROOT = "138875005";
  private static final String SIMPLE_MAP_TYPE = "900000000000496009";
  private static final String MAP_TO_SNOMED_TYPE = "1187636009";
  private static final String ICD_O_MAP = "446608001";
  private static final String CTV3_MAP = "900000000000497000";
  private static final String ARTG_MAP = "11000168105";
  private static final String GMDN_MAP = "467614008";
  private static final String ICD_10_EXTENDED_MAP = "447562003";
  private static final String MEDDRA_TO_SNOMED_MAP = "1193497006";

  // The target systems of the three simple maps with a known target system.
  private static final String ICD_O_SYSTEM = "http://hl7.org/fhir/sid/icd-o-3";
  private static final String CTV3_SYSTEM = "http://read.info/ctv3";
  private static final String ARTG_SYSTEM =
      "https://www.tga.gov.au/australian-register-therapeutic-goods";

  // Synthetic concepts.
  private static final String FINDING = "100000";
  private static final String SOURCE_A = "110000";
  private static final String SOURCE_B = "120000";
  private static final String SOURCE_C = "130000";
  private static final String SOURCE_D = "140000";
  private static final String SOURCE_E = "150000";

  private static LocalTerminologyService service;

  @BeforeAll
  static void setUp(@TempDir final Path work) {
    final SparkSession spark =
        SparkSession.builder()
            .appName("LocalTerminologyServiceSimpleMapTest")
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
    final String store = work.resolve("store").toString();
    new SnomedRf2Importer(spark, store)
        .importFrom(writeRelease(work.resolve("release")).toString(), null);
    service =
        new LocalTerminologyService(
            TerminologyConfiguration.builder()
                .mode(TerminologyMode.LOCAL)
                .local(LocalTerminologyConfiguration.builder().storagePath(store).build())
                .build(),
            Map.of());
  }

  @AfterAll
  static void tearDown() {
    if (service != null) {
      service.close();
    }
  }

  // --- Forward translation. ---

  /**
   * Each simple map Ontoserver supports, with a source, its single target, and the equivalence
   * Ontoserver gives the map.
   */
  static Stream<Arguments> knownSimpleMaps() {
    return Stream.of(
        arguments(ICD_O_MAP, SOURCE_B, "inexact", ICD_O_SYSTEM, "C18.9"),
        // CTV3 is outside the simple map type hierarchy here, as it is in current releases.
        arguments(CTV3_MAP, SOURCE_D, "equivalent", CTV3_SYSTEM, "X40J4"),
        arguments(ARTG_MAP, SOURCE_E, "inexact", ARTG_SYSTEM, "272659"));
  }

  @ParameterizedTest
  @MethodSource("knownSimpleMaps")
  void forwardTranslationCarriesTheKnownTargetSystemAndEquivalence(
      final String refset,
      final String source,
      final String equivalence,
      final String system,
      final String target) {
    assertEquals(
        List.of(equivalence + " " + system + "|" + target),
        describe(service.translate(snomed(source), conceptMap(refset), false, null)));
  }

  @Test
  void reverseTranslationCarriesTheKnownEquivalence() {
    // The equivalence of a known map holds in the reverse direction too.
    assertEquals(
        List.of("equivalent " + SNOMED + "|" + SOURCE_D),
        describe(
            service.translate(coding(CTV3_SYSTEM, "X40J4"), conceptMap(CTV3_MAP), true, null)));
  }

  @Test
  void forwardTranslationReturnsEveryTargetInCodeOrder() {
    // SOURCE_A has two ICD-O targets, written to the release with the higher code first.
    assertEquals(
        List.of("inexact " + ICD_O_SYSTEM + "|C18.9", "inexact " + ICD_O_SYSTEM + "|C19.9"),
        describe(service.translate(snomed(SOURCE_A), conceptMap(ICD_O_MAP), false, null)));
  }

  @Test
  void forwardTranslationThroughAnUnknownTargetSystemOmitsTheSystem() {
    // No target system is known for GMDN, so the target is a code with no system.
    assertEquals(
        List.of("inexact |47569"),
        describe(service.translate(snomed(SOURCE_C), conceptMap(GMDN_MAP), false, null)));
  }

  @Test
  void forwardTranslationHonoursAnEditionQualifiedConceptMapUrl() {
    // The base of the URL names the edition and version, as THO allows.
    final String url =
        SNOMED + "/" + MODULE + "/version/" + EFFECTIVE_TIME + "?fhir_cm=" + ICD_O_MAP;
    assertEquals(
        List.of("inexact " + ICD_O_SYSTEM + "|C18.9"),
        describe(service.translate(snomed(SOURCE_B), url, false, null)));
  }

  @Test
  void forwardTranslationOfANonSnomedCodingFindsNothing() {
    // The source of a simple map is always a SNOMED CT concept.
    assertEquals(
        List.of(),
        describe(
            service.translate(coding(ICD_O_SYSTEM, SOURCE_B), conceptMap(ICD_O_MAP), false, null)));
  }

  // --- Reverse translation. ---

  @Test
  void reverseTranslationReturnsEverySourceInCodeOrder() {
    // Both SOURCE_A and SOURCE_B map to C18.9. The result is SNOMED CT concepts, still inexact.
    assertEquals(
        List.of("inexact " + SNOMED + "|" + SOURCE_A, "inexact " + SNOMED + "|" + SOURCE_B),
        describe(
            service.translate(coding(ICD_O_SYSTEM, "C18.9"), conceptMap(ICD_O_MAP), true, null)));
  }

  @Test
  void reverseTranslationRequiresTheKnownTargetSystem() {
    // A code in any other system is not a target of a map whose target system is known.
    assertEquals(
        List.of(),
        describe(
            service.translate(
                coding("http://example.org", "C18.9"), conceptMap(ICD_O_MAP), true, null)));
  }

  @Test
  void reverseTranslationThroughAnUnknownTargetSystemMatchesTheCodeAlone() {
    // With no target system to compare against, the code alone identifies the target.
    assertEquals(
        List.of("inexact " + SNOMED + "|" + SOURCE_C),
        describe(
            service.translate(
                coding("http://example.org", "47569"), conceptMap(GMDN_MAP), true, null)));
  }

  // --- Reference sets that are not simple maps. ---

  @Test
  void unknownReferenceSetOutsideTheSimpleMapHierarchyIsUnknownContent() {
    // MedDRA to SNOMED CT ships its rows in a simple map file, but it is neither a simple map type
    // reference set nor one of the maps Ontoserver supports.
    assertEquals(
        List.of(),
        describe(
            service.translate(snomed(SOURCE_C), conceptMap(MEDDRA_TO_SNOMED_MAP), false, null)));
  }

  @Test
  void extendedMapTargetsAreNotTranslated() {
    // The extended map sits under the simple map type here, so only the importer, which reads map
    // targets from simple map files alone, stands between its rule-dependent targets and a result.
    assertEquals(
        List.of(),
        describe(
            service.translate(snomed(SOURCE_C), conceptMap(ICD_10_EXTENDED_MAP), false, null)));
  }

  // --- Helpers. ---

  @Nonnull
  private static Coding snomed(@Nonnull final String code) {
    return coding(SNOMED, code);
  }

  @Nonnull
  private static Coding coding(@Nonnull final String system, @Nonnull final String code) {
    return new Coding().setSystem(system).setCode(code);
  }

  @Nonnull
  private static String conceptMap(@Nonnull final String refset) {
    return SNOMED + "?fhir_cm=" + refset;
  }

  /**
   * Renders translations as {@code equivalence system|code}, in the order they were returned, with
   * an empty system where the target has none.
   */
  @Nonnull
  private static List<String> describe(@Nonnull final List<Translation> translations) {
    return translations.stream()
        .map(
            t ->
                t.getEquivalence().toCode()
                    + " "
                    + Objects.toString(t.getConcept().getSystem(), "")
                    + "|"
                    + t.getConcept().getCode())
        .toList();
  }

  /**
   * Writes the purpose-built release: concepts, one FSN each, the is-a hierarchy, a simple map file
   * and an extended map file.
   */
  @Nonnull
  private static Path writeRelease(@Nonnull final Path release) {
    final String[][] edges = {
      {SIMPLE_MAP_TYPE, ROOT},
      {ICD_O_MAP, SIMPLE_MAP_TYPE},
      {ARTG_MAP, SIMPLE_MAP_TYPE},
      {GMDN_MAP, SIMPLE_MAP_TYPE},
      {ICD_10_EXTENDED_MAP, SIMPLE_MAP_TYPE},
      {MAP_TO_SNOMED_TYPE, ROOT},
      {CTV3_MAP, MAP_TO_SNOMED_TYPE},
      {MEDDRA_TO_SNOMED_MAP, MAP_TO_SNOMED_TYPE},
      {FINDING, ROOT},
      {SOURCE_A, FINDING},
      {SOURCE_B, FINDING},
      {SOURCE_C, FINDING},
      {SOURCE_D, FINDING},
      {SOURCE_E, FINDING}
    };
    final StringBuilder concepts =
        new StringBuilder("id\teffectiveTime\tactive\tmoduleId\tdefinitionStatusId\r\n");
    final StringBuilder descriptions =
        new StringBuilder(
            "id\teffectiveTime\tactive\tmoduleId\tconceptId\tlanguageCode\ttypeId\tterm\t"
                + "caseSignificanceId\r\n");
    final StringBuilder relationships =
        new StringBuilder(
            "id\teffectiveTime\tactive\tmoduleId\tsourceId\tdestinationId\trelationshipGroup\t"
                + "typeId\tcharacteristicTypeId\tmodifierId\r\n");
    int identifier = 0;
    concepts.append(row(ROOT, EFFECTIVE_TIME, "1", MODULE, "900000000000074008"));
    for (final String[] edge : edges) {
      final String code = edge[0];
      concepts.append(row(code, EFFECTIVE_TIME, "1", MODULE, "900000000000074008"));
      descriptions.append(
          row(
              "d" + ++identifier,
              EFFECTIVE_TIME,
              "1",
              MODULE,
              code,
              "en",
              "900000000000003001",
              "Concept " + code + " (foundation metadata concept)",
              "900000000000448009"));
      relationships.append(
          row(
              "r" + identifier,
              EFFECTIVE_TIME,
              "1",
              MODULE,
              code,
              edge[1],
              "0",
              IS_A,
              "900000000000011006",
              "900000000000451002"));
    }
    // Rows that share a source or a target are written against code order, so that nothing can
    // take the order of a result from this file.
    final String simpleMap =
        row(
                "id",
                "effectiveTime",
                "active",
                "moduleId",
                "refsetId",
                "referencedComponentId",
                "mapTarget")
            + simpleMapRow(1, ICD_O_MAP, SOURCE_B, "C18.9")
            + simpleMapRow(2, ICD_O_MAP, SOURCE_A, "C19.9")
            + simpleMapRow(3, ICD_O_MAP, SOURCE_A, "C18.9")
            + simpleMapRow(4, CTV3_MAP, SOURCE_D, "X40J4")
            + simpleMapRow(5, ARTG_MAP, SOURCE_E, "272659")
            + simpleMapRow(6, GMDN_MAP, SOURCE_C, "47569")
            + simpleMapRow(7, MEDDRA_TO_SNOMED_MAP, SOURCE_C, "47569");
    final String extendedMap =
        row(
                "id",
                "effectiveTime",
                "active",
                "moduleId",
                "refsetId",
                "referencedComponentId",
                "mapGroup",
                "mapPriority",
                "mapRule",
                "mapAdvice",
                "mapTarget",
                "correlationId",
                "mapCategoryId")
            + row(
                "e1",
                EFFECTIVE_TIME,
                "1",
                MODULE,
                ICD_10_EXTENDED_MAP,
                SOURCE_C,
                "1",
                "1",
                "TRUE",
                "ALWAYS E11.9",
                "E11.9",
                "447561005",
                "447637006");
    final Path terminology = release.resolve("Snapshot").resolve("Terminology");
    final Path content = release.resolve("Snapshot").resolve("Refset").resolve("Map");
    try {
      Files.createDirectories(terminology);
      Files.createDirectories(content);
      Files.writeString(
          terminology.resolve("sct2_Concept_Snapshot_INT_" + EFFECTIVE_TIME + ".txt"),
          concepts.toString());
      Files.writeString(
          terminology.resolve("sct2_Description_Snapshot-en_INT_" + EFFECTIVE_TIME + ".txt"),
          descriptions.toString());
      Files.writeString(
          terminology.resolve("sct2_Relationship_Snapshot_INT_" + EFFECTIVE_TIME + ".txt"),
          relationships.toString());
      Files.writeString(
          content.resolve("der2_sRefset_SimpleMapSnapshot_INT_" + EFFECTIVE_TIME + ".txt"),
          simpleMap);
      Files.writeString(
          content.resolve("der2_iisssccRefset_ExtendedMapSnapshot_INT_" + EFFECTIVE_TIME + ".txt"),
          extendedMap);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return release;
  }

  @Nonnull
  private static String simpleMapRow(
      final int id,
      @Nonnull final String refset,
      @Nonnull final String source,
      @Nonnull final String target) {
    return row("s" + id, EFFECTIVE_TIME, "1", MODULE, refset, source, target);
  }

  /** Joins RF2 fields into one tab-delimited, CRLF-terminated row. */
  @Nonnull
  private static String row(@Nonnull final String... fields) {
    return String.join("\t", fields) + "\r\n";
  }
}
