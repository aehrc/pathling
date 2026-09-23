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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContentException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLimitExceededException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapRelationship;
import au.csiro.pathling.terminology.conceptmap.ConceptMapVersionException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Service-level tests for reading concept maps in local mode.
 *
 * <p>Explicit maps: pinned and unpinned version selection over imported ConceptMap resources,
 * agreement with {@link ConceptMapContent#fromResource}, unknown URLs, undeterminable versions and
 * unrepresentable content.
 *
 * <p>SNOMED CT implicit maps ({@code ?fhir_cm=}): the base release's only association reference set
 * is SAME AS, so these run against a second store imported from a copy of the base release whose
 * association file gains REPLACED BY, POSSIBLY EQUIVALENT TO and ALTERNATIVE rows, written out of
 * code order, together with the unmodified later release, which holds SAME AS rows only. The
 * store's default SNOMED CT version is therefore the later release, and the extended rows are
 * reached through the base release's edition/version URI.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceConceptMapTest {

  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private static final String SAME_AS = Rf2Mini.SAME_AS_REFSET;
  private static final String REPLACED_BY = "900000000000526001";
  private static final String POSSIBLY_EQUIVALENT_TO = "900000000000523009";
  private static final String ALTERNATIVE = "900000000000530003";

  /** An association reference set for which THO defines no implicit concept map. */
  private static final String MOVED_FROM = "900000000000497000";

  /** An association target that is not in the release's concept dictionary. */
  private static final String ABSENT_CONCEPT = "9999999003";

  /** An edition/version URI that the implicit map store does not hold. */
  private static final String UNHELD_VERSION =
      "http://snomed.info/sct/900000000000207008/version/20990101";

  private static TerminologyService service;
  private static LocalTerminologyService implicitService;

  @BeforeAll
  static void setUp(@TempDir final Path work) {
    service = ConceptMapTerminologyFixture.service();
    implicitService = serviceOver(importImplicitMapStore(work));
  }

  @AfterAll
  static void tearDown() {
    if (implicitService != null) {
      implicitService.close();
      implicitService = null;
    }
  }

  @Test
  void pinnedVersionsYieldTheirOwnRows() {
    final ConceptMapContent content2025 =
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2025", NO_LIMIT)
            .orElseThrow();
    final ConceptMapContent content2026 =
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2026", NO_LIMIT)
            .orElseThrow();

    assertEquals("2025", content2025.getVersion());
    assertEquals("2026", content2026.getVersion());
    assertEquals(
        List.of(ConceptMapTerminologyFixture.EDITED_2025_TARGET),
        targetCodesOf(content2025, "73211009"));
    assertEquals(List.of("E14"), targetCodesOf(content2026, "73211009"));
  }

  @Test
  void unpinnedSelectsTheLatestVersion() {
    final ConceptMapContent content =
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, null, NO_LIMIT)
            .orElseThrow();

    assertEquals("2026", content.getVersion());
    assertEquals(List.of("E14"), targetCodesOf(content, "73211009"));
  }

  @Test
  void contentEqualsConversionOfTheImportedResource() {
    assertEquals(
        ConceptMapContent.fromResource(ConceptMapTerminologyFixture.conceptMap2026(), NO_LIMIT),
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, null, NO_LIMIT)
            .orElseThrow());
    assertEquals(
        ConceptMapContent.fromResource(ConceptMapTerminologyFixture.conceptMap2025(), NO_LIMIT),
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2025", NO_LIMIT)
            .orElseThrow());
  }

  @Test
  void returnsEmptyForUnknownUrl() {
    assertTrue(
        service.readConceptMap("http://example.org/ConceptMap/missing", null, NO_LIMIT).isEmpty());
  }

  @Test
  void returnsEmptyForUnknownPinnedVersion() {
    assertTrue(
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2024", NO_LIMIT)
            .isEmpty());
  }

  @Test
  void rejectsUndeterminableVersionOrder() {
    final ConceptMapVersionException e =
        assertThrows(
            ConceptMapVersionException.class,
            () -> service.readConceptMap(ConceptMapTerminologyFixture.AMBIGUOUS, null, NO_LIMIT));

    assertTrue(e.getMessage().contains(ConceptMapTerminologyFixture.AMBIGUOUS), e.getMessage());
  }

  @Test
  void rejectsMapWithDependsOn() {
    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> service.readConceptMap(ConceptMapTerminologyFixture.DEPENDS_ON, null, NO_LIMIT));

    assertTrue(e.getMessage().contains("dependsOn"), e.getMessage());
  }

  @Test
  void sameAsYieldsOneRowPerAssociationRow() {
    final ConceptMapContent content = readImplicit(Rf2Mini.VERSION_20230601, SAME_AS);

    final String v = Rf2Mini.VERSION_20230601;
    final String type2 = "Type 2 diabetes mellitus";
    final String equivalent = ConceptMapRelationship.EQUIVALENT;
    assertEquals(
        List.of(
            row(
                v,
                Rf2Mini.DIABETES_INACTIVE,
                "Diabetes",
                Rf2Mini.TYPE2_DIABETES,
                type2,
                equivalent),
            row(
                v,
                Rf2Mini.ASSOCIATED_FILLER_1,
                "Mini diabetes subtype 86",
                Rf2Mini.TYPE2_DIABETES,
                type2,
                equivalent),
            row(
                v,
                Rf2Mini.ASSOCIATED_FILLER_2,
                "Mini other disorder 36",
                Rf2Mini.TYPE2_DIABETES,
                type2,
                equivalent),
            row(
                v,
                Rf2Mini.ASSOCIATED_FILLER_3,
                "Mini other disorder 56",
                Rf2Mini.TYPE2_DIABETES,
                type2,
                equivalent)),
        content.getMappings());
  }

  @Test
  void replacedByRowsAreEquivalentAndOrderedBySourceCode() {
    final ConceptMapContent content = readImplicit(Rf2Mini.VERSION_20230601, REPLACED_BY);

    final String v = Rf2Mini.VERSION_20230601;
    assertEquals(
        List.of(
            row(
                v,
                Rf2Mini.DIABETES_INACTIVE,
                "Diabetes",
                Rf2Mini.TYPE2_DIABETES,
                "Type 2 diabetes mellitus",
                ConceptMapRelationship.EQUIVALENT),
            row(
                v,
                Rf2Mini.ASSOCIATED_FILLER_2,
                "Mini other disorder 36",
                Rf2Mini.DIABETES,
                "Diabetes mellitus",
                ConceptMapRelationship.EQUIVALENT)),
        content.getMappings());
  }

  @Test
  void possiblyEquivalentToYieldsEveryTargetOfAConcept() {
    // The concept has three targets, written out of code order, one of which is not in the
    // dictionary and so has no display.
    final ConceptMapContent content =
        readImplicit(Rf2Mini.VERSION_20230601, POSSIBLY_EQUIVALENT_TO);

    final String v = Rf2Mini.VERSION_20230601;
    final String gestational = "Gestational diabetes mellitus";
    final String relatedTo = ConceptMapRelationship.RELATED_TO;
    assertEquals(
        List.of(
            row(
                v,
                Rf2Mini.GESTATIONAL_DIABETES,
                gestational,
                Rf2Mini.TYPE1_DIABETES,
                "Type 1 diabetes mellitus",
                relatedTo),
            row(
                v,
                Rf2Mini.GESTATIONAL_DIABETES,
                gestational,
                Rf2Mini.TYPE2_WITH_COMPLICATION,
                "Type 2 diabetes mellitus with complication",
                relatedTo),
            row(v, Rf2Mini.GESTATIONAL_DIABETES, gestational, ABSENT_CONCEPT, null, relatedTo)),
        content.getMappings());
  }

  @Test
  void alternativeRowsAreRelatedTo() {
    final ConceptMapContent content = readImplicit(Rf2Mini.VERSION_20230601, ALTERNATIVE);

    final String v = Rf2Mini.VERSION_20230601;
    assertEquals(
        List.of(
            row(
                v,
                Rf2Mini.HYPERTENSION,
                "Hypertensive disorder",
                Rf2Mini.DISORDER,
                "Mini disorder",
                ConceptMapRelationship.RELATED_TO),
            row(
                v,
                Rf2Mini.DIABETES_INACTIVE,
                "Diabetes",
                Rf2Mini.DIABETES,
                "Diabetes mellitus",
                ConceptMapRelationship.RELATED_TO)),
        content.getMappings());
  }

  @Test
  void implicitContentCarriesTheUrlAndTheVersionUri() {
    final String url = implicitUrl(Rf2Mini.VERSION_20230601, REPLACED_BY);
    final ConceptMapContent content =
        implicitService.readConceptMap(url, null, NO_LIMIT).orElseThrow();

    assertEquals(url, content.getUrl());
    assertEquals(Rf2Mini.VERSION_20230601, content.getVersion());
  }

  @Test
  void bareBaseSelectsTheDefaultVersion() {
    // The default is the later release, whose SAME AS rows carry its version URI and which holds
    // no REPLACED BY rows at all.
    final ConceptMapContent sameAs = readImplicit(Rf2Mini.SNOMED_URI, SAME_AS);
    assertEquals(Rf2Mini.VERSION_20240601, sameAs.getVersion());
    assertEquals(4, sameAs.getMappings().size());
    assertTrue(
        sameAs.getMappings().stream()
            .allMatch(
                mapping ->
                    Rf2Mini.VERSION_20240601.equals(mapping.getSourceVersion())
                        && Rf2Mini.VERSION_20240601.equals(mapping.getTargetVersion())));

    final ConceptMapContent replacedBy = readImplicit(Rf2Mini.SNOMED_URI, REPLACED_BY);
    assertEquals(Rf2Mini.VERSION_20240601, replacedBy.getVersion());
    assertTrue(replacedBy.getMappings().isEmpty());
  }

  @Test
  void editionVersionBaseSelectsThatVersion() {
    assertEquals(2, readImplicit(Rf2Mini.VERSION_20230601, REPLACED_BY).getMappings().size());
    assertTrue(readImplicit(Rf2Mini.VERSION_20240601, REPLACED_BY).getMappings().isEmpty());
  }

  @Test
  void unheldVersionReturnsEmpty() {
    assertTrue(
        implicitService
            .readConceptMap(implicitUrl(UNHELD_VERSION, SAME_AS), null, NO_LIMIT)
            .isEmpty());
  }

  @Test
  void storeWithoutSnomedCtReturnsEmpty() {
    assertTrue(
        FhirTerminologyFixture.service()
            .readConceptMap(implicitUrl(Rf2Mini.SNOMED_URI, SAME_AS), null, NO_LIMIT)
            .isEmpty());
  }

  @Test
  void otherReferenceSetReturnsEmpty() {
    assertTrue(
        implicitService
            .readConceptMap(implicitUrl(Rf2Mini.SNOMED_URI, MOVED_FROM), null, NO_LIMIT)
            .isEmpty());
    assertTrue(
        implicitService
            .readConceptMap(implicitUrl(Rf2Mini.SNOMED_URI, Rf2Mini.SIMPLE_REFSET), null, NO_LIMIT)
            .isEmpty());
  }

  @Test
  void pinnedImplicitMapIsRejected() {
    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () ->
                implicitService.readConceptMap(
                    implicitUrl(Rf2Mini.SNOMED_URI, REPLACED_BY), "x", NO_LIMIT));

    assertEquals(
        "cannot determine which version to use: an implicit concept map URL carries its version"
            + " in its base",
        e.getMessage());
  }

  @Test
  void implicitMapOverTheLimitIsRejected() {
    assertThrows(
        ConceptMapLimitExceededException.class,
        () ->
            implicitService.readConceptMap(
                implicitUrl(Rf2Mini.VERSION_20230601, SAME_AS), null, 3));
  }

  @Nonnull
  private static ConceptMapContent readImplicit(
      @Nonnull final String base, @Nonnull final String refset) {
    return implicitService.readConceptMap(implicitUrl(base, refset), null, NO_LIMIT).orElseThrow();
  }

  @Nonnull
  private static String implicitUrl(@Nonnull final String base, @Nonnull final String refset) {
    return base + "?fhir_cm=" + refset;
  }

  @Nonnull
  private static ConceptMapping row(
      @Nonnull final String version,
      @Nonnull final String source,
      @Nonnull final String sourceDisplay,
      @Nonnull final String target,
      @Nullable final String targetDisplay,
      @Nonnull final String relationship) {
    return new ConceptMapping(
        Rf2Mini.SNOMED_URI,
        version,
        source,
        sourceDisplay,
        Rf2Mini.SNOMED_URI,
        version,
        target,
        targetDisplay,
        relationship);
  }

  @Nonnull
  private static LocalTerminologyService serviceOver(@Nonnull final String storagePath) {
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath(storagePath).build())
            .build();
    return new LocalTerminologyService(configuration, Map.of());
  }

  /**
   * Imports the extended copy of the base release and the unmodified later release into one store,
   * and returns the path of the store.
   */
  @Nonnull
  private static String importImplicitMapStore(@Nonnull final Path work) {
    final Path release =
        Rf2MiniReleaseCopy.withAssociationRows(
            work.resolve("release"),
            List.of(
                Rf2MiniReleaseCopy.associationRow(
                    "00000000-0000-4000-8000-000000000340",
                    REPLACED_BY,
                    Rf2Mini.ASSOCIATED_FILLER_2,
                    Rf2Mini.DIABETES),
                Rf2MiniReleaseCopy.associationRow(
                    "00000000-0000-4000-8000-000000000341",
                    REPLACED_BY,
                    Rf2Mini.DIABETES_INACTIVE,
                    Rf2Mini.TYPE2_DIABETES),
                Rf2MiniReleaseCopy.associationRow(
                    "00000000-0000-4000-8000-000000000342",
                    POSSIBLY_EQUIVALENT_TO,
                    Rf2Mini.GESTATIONAL_DIABETES,
                    ABSENT_CONCEPT),
                Rf2MiniReleaseCopy.associationRow(
                    "00000000-0000-4000-8000-000000000343",
                    POSSIBLY_EQUIVALENT_TO,
                    Rf2Mini.GESTATIONAL_DIABETES,
                    Rf2Mini.TYPE2_WITH_COMPLICATION),
                Rf2MiniReleaseCopy.associationRow(
                    "00000000-0000-4000-8000-000000000344",
                    POSSIBLY_EQUIVALENT_TO,
                    Rf2Mini.GESTATIONAL_DIABETES,
                    Rf2Mini.TYPE1_DIABETES),
                Rf2MiniReleaseCopy.associationRow(
                    "00000000-0000-4000-8000-000000000345",
                    ALTERNATIVE,
                    Rf2Mini.HYPERTENSION,
                    Rf2Mini.DISORDER),
                Rf2MiniReleaseCopy.associationRow(
                    "00000000-0000-4000-8000-000000000346",
                    ALTERNATIVE,
                    Rf2Mini.DIABETES_INACTIVE,
                    Rf2Mini.DIABETES)));
    final SparkSession spark =
        SparkSession.builder()
            .appName("LocalTerminologyServiceConceptMapTest")
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
    final SnomedRf2Importer importer = new SnomedRf2Importer(spark, store);
    importer.importFrom(release.toString(), null);
    importer.importFrom(Rf2Mini.releasePath("international-20240601").toString(), null);
    return store;
  }

  private static List<String> targetCodesOf(
      final ConceptMapContent content, final String sourceCode) {
    return content.getMappings().stream()
        .filter(mapping -> sourceCode.equals(mapping.getSourceCode()))
        .map(ConceptMapping::getTargetCode)
        .toList();
  }
}
