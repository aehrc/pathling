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
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.store.SnomedRf2Importer;
import au.csiro.pathling.test.FhirFixtures;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests local {@code translate}: explicit imported ConceptMaps (forward, reverse, target-scoped,
 * with equivalences preserved), the unknown-content fallback, and SNOMED implicit concept maps
 * derived from association reference sets.
 *
 * <p>The shared fixture's only association reference set is SAME AS, in which every referenced
 * concept has one target. The tests of a concept with several targets therefore run against a
 * second store, imported from a copy of the base release whose association file gains a POSSIBLY
 * EQUIVALENT TO referenced concept with three targets.
 *
 * <p>The ordering assertions here pin the contract, but they do not by themselves prove the service
 * imposes it, because this store's reference set rows already happen to be laid out in code order.
 * {@link LocalTerminologyServiceRefsetLayoutTest} is what proves that, against a store laid out the
 * other way round.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceTranslateTest {

  /** The fixture's SAME AS association reference set, as an implicit concept map URL. */
  private static final String SAME_AS_CONCEPT_MAP =
      Rf2Mini.SNOMED_URI + "?fhir_cm=" + Rf2Mini.SAME_AS_REFSET;

  /** The POSSIBLY EQUIVALENT TO association reference set, which the extended release copy adds. */
  private static final String POSSIBLY_EQUIVALENT_TO_REFSET = "900000000000523009";

  private static final String POSSIBLY_EQUIVALENT_TO_CONCEPT_MAP =
      Rf2Mini.SNOMED_URI + "?fhir_cm=" + POSSIBLY_EQUIVALENT_TO_REFSET;

  /** The concept the extended release copy gives several POSSIBLY EQUIVALENT TO targets. */
  private static final String AMBIGUOUS_CONCEPT = Rf2Mini.GESTATIONAL_DIABETES;

  /**
   * The targets of {@link #AMBIGUOUS_CONCEPT} in code order, which is not the order the extended
   * release copy writes their rows in.
   */
  private static final List<String> AMBIGUOUS_TARGETS_IN_CODE_ORDER =
      List.of(Rf2Mini.TYPE1_DIABETES, Rf2Mini.TYPE2_DIABETES, Rf2Mini.TYPE2_WITH_COMPLICATION);

  private static TerminologyService fhirService;
  private static TerminologyService snomedService;
  private static LocalTerminologyService extendedService;

  @BeforeAll
  static void setUp(@TempDir final Path work) {
    fhirService = FhirTerminologyFixture.service();
    snomedService = serviceOver(LocalTerminologyFixture.storagePath());
    extendedService = serviceOver(importExtendedRelease(work));
  }

  @AfterAll
  static void tearDown() {
    if (extendedService != null) {
      extendedService.close();
      extendedService = null;
    }
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
   * Imports a copy of the base release whose association reference set file gains three POSSIBLY
   * EQUIVALENT TO rows for {@link #AMBIGUOUS_CONCEPT}, written out of code order, and returns the
   * path of the store.
   */
  @Nonnull
  private static String importExtendedRelease(@Nonnull final Path work) {
    final Path release = copyOfBaseRelease(work.resolve("release"));
    appendAssociationRows(
        release,
        List.of(
            associationRow(
                "00000000-0000-4000-8000-00000000032f",
                AMBIGUOUS_CONCEPT,
                Rf2Mini.TYPE2_WITH_COMPLICATION),
            associationRow(
                "00000000-0000-4000-8000-000000000330", AMBIGUOUS_CONCEPT, Rf2Mini.TYPE1_DIABETES),
            associationRow(
                "00000000-0000-4000-8000-000000000331",
                AMBIGUOUS_CONCEPT,
                Rf2Mini.TYPE2_DIABETES)));
    final SparkSession spark =
        SparkSession.builder()
            .appName("LocalTerminologyServiceTranslateTest")
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
    new SnomedRf2Importer(spark, store).importFrom(release.toString(), null);
    return store;
  }

  /** An active POSSIBLY EQUIVALENT TO association row of the base release. */
  @Nonnull
  private static String associationRow(
      @Nonnull final String id, @Nonnull final String referenced, @Nonnull final String target) {
    return String.join(
        "\t",
        id,
        "20230601",
        "1",
        Rf2Mini.CORE_MODULE,
        POSSIBLY_EQUIVALENT_TO_REFSET,
        referenced,
        target);
  }

  /** Copies the base release into a directory. */
  @Nonnull
  private static Path copyOfBaseRelease(@Nonnull final Path release) {
    try (final Stream<Path> paths = Files.walk(Rf2Mini.baseRelease())) {
      for (final Path source : paths.sorted().toList()) {
        final Path target = release.resolve(Rf2Mini.baseRelease().relativize(source).toString());
        if (Files.isDirectory(source)) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(source, target);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return release;
  }

  /** Appends data rows to the release's association reference set file. */
  private static void appendAssociationRows(
      @Nonnull final Path release, @Nonnull final List<String> rows) {
    try (final Stream<Path> paths = Files.walk(release)) {
      final Path file =
          paths
              .filter(path -> path.getFileName().toString().startsWith("der2_cRefset_Association"))
              .min(Comparator.naturalOrder())
              .orElseThrow(() -> new IllegalStateException("No association reference set file"));
      final List<String> lines = new ArrayList<>(Files.readAllLines(file));
      lines.addAll(rows);
      Files.write(file, lines);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Coding species(final String code) {
    return new Coding().setSystem(FhirFixtures.ANIMAL_SPECIES).setCode(code);
  }

  private static Coding category(final String code) {
    return new Coding().setSystem(FhirFixtures.ANIMAL_CATEGORY).setCode(code);
  }

  private static Coding snomed(final String code) {
    return new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(code);
  }

  private static Set<String> targetCodes(final List<Translation> translations) {
    return translations.stream().map(t -> t.getConcept().getCode()).collect(Collectors.toSet());
  }

  /** The translated codes in the order they were returned, for assertions that are about order. */
  private static List<String> orderedTargetCodes(final List<Translation> translations) {
    return translations.stream().map(t -> t.getConcept().getCode()).toList();
  }

  private static String eclValueSet(final String ecl) {
    return Rf2Mini.SNOMED_URI + "?fhir_vs=ecl/" + URLEncoder.encode(ecl, StandardCharsets.UTF_8);
  }

  @Test
  void translatesForward() {
    final List<Translation> result =
        fhirService.translate(species(FhirFixtures.DOG), FhirFixtures.CONCEPT_MAP, false, null);
    assertEquals(1, result.size());
    assertEquals("pet", result.get(0).getConcept().getCode());
    assertEquals(FhirFixtures.ANIMAL_CATEGORY, result.get(0).getConcept().getSystem());
    assertEquals(ConceptMapEquivalence.EQUIVALENT, result.get(0).getEquivalence());
  }

  @Test
  void translatesReverse() {
    // Everything that maps to the "pet" category: dog, cat, and sparrow.
    final List<Translation> result =
        fhirService.translate(category("pet"), FhirFixtures.CONCEPT_MAP, true, null);
    assertEquals(
        Set.of(FhirFixtures.DOG, FhirFixtures.CAT, FhirFixtures.SPARROW), targetCodes(result));
  }

  @Test
  void reverseTranslationInvertsEquivalence() {
    // Sparrow maps to "pet" with a "wider" equivalence; the reverse mapping is "narrower".
    final List<Translation> result =
        fhirService.translate(category("pet"), FhirFixtures.CONCEPT_MAP, true, null);
    final ConceptMapEquivalence sparrowEquivalence =
        result.stream()
            .filter(t -> FhirFixtures.SPARROW.equals(t.getConcept().getCode()))
            .map(Translation::getEquivalence)
            .findFirst()
            .orElseThrow();
    assertEquals(ConceptMapEquivalence.NARROWER, sparrowEquivalence);
  }

  @Test
  void translatesWithTargetValueSetFilter() {
    // The target names a value set: dog maps to "pet", which is a member of the pets value set.
    final List<Translation> matching =
        fhirService.translate(
            species(FhirFixtures.DOG), FhirFixtures.CONCEPT_MAP, false, FhirFixtures.VS_PETS);
    assertEquals(Set.of("pet"), targetCodes(matching));

    // A target value set that does not contain the mapped concept excludes the translation.
    final List<Translation> nonMatching =
        fhirService.translate(
            species(FhirFixtures.DOG),
            FhirFixtures.CONCEPT_MAP,
            false,
            FhirFixtures.VS_MAMMALS_ENUMERATED);
    assertTrue(nonMatching.isEmpty());
  }

  @Test
  void unknownConceptMapReturnsEmpty() {
    assertTrue(
        fhirService
            .translate(
                species(FhirFixtures.DOG),
                "http://example.org/fhir/ConceptMap/does-not-exist",
                false,
                null)
            .isEmpty());
  }

  @Test
  void translatesSnomedAssociationRefsetForward() {
    // The inactive concept has a SAME AS association to its active replacement.
    final String conceptMap = Rf2Mini.SNOMED_URI + "?fhir_cm=" + Rf2Mini.SAME_AS_REFSET;
    final List<Translation> result =
        snomedService.translate(snomed(Rf2Mini.DIABETES_INACTIVE), conceptMap, false, null);
    assertEquals(Set.of(Rf2Mini.TYPE2_DIABETES), targetCodes(result));
    assertEquals(Rf2Mini.SNOMED_URI, result.get(0).getConcept().getSystem());
  }

  @Test
  void translatesSnomedAssociationRefsetReverse() {
    // Four concepts are associated with this target, and the results must come back in ascending
    // code order.
    final List<Translation> result =
        snomedService.translate(snomed(Rf2Mini.TYPE2_DIABETES), SAME_AS_CONCEPT_MAP, true, null);
    assertEquals(
        List.of(
            Rf2Mini.DIABETES_INACTIVE,
            Rf2Mini.ASSOCIATED_FILLER_1,
            Rf2Mini.ASSOCIATED_FILLER_2,
            Rf2Mini.ASSOCIATED_FILLER_3),
        orderedTargetCodes(result));
  }

  @Test
  void targetValueSetFilterPreservesTheReverseTranslationOrder() {
    // The two survivors are non-adjacent in the unfiltered result, so this asserts that filtering
    // preserves the established sequence rather than merely selecting the right members.
    final String valueSet =
        eclValueSet(Rf2Mini.ASSOCIATED_FILLER_1 + " OR " + Rf2Mini.ASSOCIATED_FILLER_3);
    final List<Translation> result =
        snomedService.translate(
            snomed(Rf2Mini.TYPE2_DIABETES), SAME_AS_CONCEPT_MAP, true, valueSet);
    assertEquals(
        List.of(Rf2Mini.ASSOCIATED_FILLER_1, Rf2Mini.ASSOCIATED_FILLER_3),
        orderedTargetCodes(result));
  }

  @Test
  void reverseTranslationOfAnUnassociatedTargetReturnsEmpty() {
    // No concept is associated with this one, so there is nothing to translate back to.
    assertTrue(
        snomedService
            .translate(snomed(Rf2Mini.HYPERTENSION), SAME_AS_CONCEPT_MAP, true, null)
            .isEmpty());
  }

  @Test
  void snomedAssociationRefsetRejectsACodingFromAnotherSystem() {
    // A reference set relates SNOMED concepts, so a coding from elsewhere cannot be a member of one
    // in either direction.
    final Coding loinc = new Coding().setSystem("http://loinc.org").setCode("1234-5");
    assertTrue(snomedService.translate(loinc, SAME_AS_CONCEPT_MAP, false, null).isEmpty());
    assertTrue(snomedService.translate(loinc, SAME_AS_CONCEPT_MAP, true, null).isEmpty());
  }

  @Test
  void translatesEveryTargetOfAnAssociationForward() {
    // The concept has three POSSIBLY EQUIVALENT TO targets, and all of them come back in ascending
    // code order, whatever order their rows were written in.
    final List<Translation> result =
        extendedService.translate(
            snomed(AMBIGUOUS_CONCEPT), POSSIBLY_EQUIVALENT_TO_CONCEPT_MAP, false, null);
    assertEquals(AMBIGUOUS_TARGETS_IN_CODE_ORDER, orderedTargetCodes(result));
    assertTrue(
        result.stream().allMatch(t -> Rf2Mini.SNOMED_URI.equals(t.getConcept().getSystem())));
  }

  @Test
  void reverseTranslationMatchesAnyTargetOfAnAssociation() {
    // Each of the three targets translates back to the one concept associated with it.
    for (final String target : AMBIGUOUS_TARGETS_IN_CODE_ORDER) {
      final List<Translation> result =
          extendedService.translate(snomed(target), POSSIBLY_EQUIVALENT_TO_CONCEPT_MAP, true, null);
      assertEquals(List.of(AMBIGUOUS_CONCEPT), orderedTargetCodes(result), target);
    }
  }

  @Test
  void extendedStoreKeepsTheSingleTargetAssociations() {
    // The added rows belong to another reference set, so SAME AS answers exactly as it does from
    // the shared fixture store, forward and in reverse.
    final List<Translation> forward =
        extendedService.translate(
            snomed(Rf2Mini.DIABETES_INACTIVE), SAME_AS_CONCEPT_MAP, false, null);
    assertEquals(List.of(Rf2Mini.TYPE2_DIABETES), orderedTargetCodes(forward));
    final List<Translation> reverse =
        extendedService.translate(snomed(Rf2Mini.TYPE2_DIABETES), SAME_AS_CONCEPT_MAP, true, null);
    assertEquals(
        List.of(
            Rf2Mini.DIABETES_INACTIVE,
            Rf2Mini.ASSOCIATED_FILLER_1,
            Rf2Mini.ASSOCIATED_FILLER_2,
            Rf2Mini.ASSOCIATED_FILLER_3),
        orderedTargetCodes(reverse));
    // The SAME AS map does not see the POSSIBLY EQUIVALENT TO rows.
    assertTrue(
        extendedService
            .translate(snomed(AMBIGUOUS_CONCEPT), SAME_AS_CONCEPT_MAP, false, null)
            .isEmpty());
  }
}
