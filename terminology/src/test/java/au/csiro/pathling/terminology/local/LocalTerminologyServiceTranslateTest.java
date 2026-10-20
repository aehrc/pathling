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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.test.FhirFixtures;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests local {@code translate}: explicit imported ConceptMaps (forward, reverse, target-scoped,
 * with equivalences preserved), the unknown-content fallback, and SNOMED implicit concept maps
 * derived from the association reference sets THO defines as such, each with its own relationship.
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

  private static TerminologyService fhirService;
  private static TerminologyService snomedService;

  @BeforeAll
  static void setUp() {
    fhirService = FhirTerminologyFixture.service();
    final TerminologyConfiguration snomedConfig =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(
                LocalTerminologyConfiguration.builder()
                    .storagePath(LocalTerminologyFixture.storagePath())
                    .build())
            .build();
    snomedService = new LocalTerminologyService(snomedConfig, Map.of());
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

  private static String implicitConceptMap(final String refsetId) {
    return Rf2Mini.SNOMED_URI + "?fhir_cm=" + refsetId;
  }

  /** The distinct equivalences of a set of translations. */
  private static Set<ConceptMapEquivalence> equivalences(final List<Translation> translations) {
    return translations.stream().map(Translation::getEquivalence).collect(Collectors.toSet());
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

  /**
   * Each association reference set that can be used as an implicit concept map, with a source
   * concept of the fixture, its target, and the relationship THO assigns to the reference set.
   */
  static Stream<Arguments> implicitConceptMaps() {
    return Stream.of(
        arguments(
            Rf2Mini.SAME_AS_REFSET,
            Rf2Mini.DIABETES_INACTIVE,
            Rf2Mini.TYPE2_DIABETES,
            ConceptMapEquivalence.EQUAL),
        arguments(
            Rf2Mini.REPLACED_BY_REFSET,
            Rf2Mini.REPLACED_BY_SOURCE,
            Rf2Mini.TYPE1_DIABETES,
            ConceptMapEquivalence.EQUIVALENT),
        arguments(
            Rf2Mini.POSSIBLY_EQUIVALENT_TO_REFSET,
            Rf2Mini.POSSIBLY_EQUIVALENT_TO_SOURCE,
            Rf2Mini.TYPE1_DIABETES,
            ConceptMapEquivalence.INEXACT),
        arguments(
            Rf2Mini.ALTERNATIVE_REFSET,
            Rf2Mini.ALTERNATIVE_SOURCE,
            Rf2Mini.GESTATIONAL_DIABETES,
            ConceptMapEquivalence.INEXACT));
  }

  @ParameterizedTest
  @MethodSource("implicitConceptMaps")
  void forwardTranslationCarriesTheRelationshipOfTheReferenceSet(
      final String refsetId,
      final String source,
      final String target,
      final ConceptMapEquivalence relationship) {
    // The relationship comes from the reference set, not from a fixed default.
    final List<Translation> result =
        snomedService.translate(snomed(source), implicitConceptMap(refsetId), false, null);
    final Translation translation =
        result.stream()
            .filter(t -> target.equals(t.getConcept().getCode()))
            .findFirst()
            .orElseThrow();
    assertEquals(Rf2Mini.SNOMED_URI, translation.getConcept().getSystem());
    assertEquals(Set.of(relationship), equivalences(result));
  }

  @ParameterizedTest
  @MethodSource("implicitConceptMaps")
  void reverseTranslationCarriesTheSameRelationship(
      final String refsetId,
      final String source,
      final String target,
      final ConceptMapEquivalence relationship) {
    // Each of these relationships is symmetric, so reversing the map does not change it.
    final List<Translation> result =
        snomedService.translate(snomed(target), implicitConceptMap(refsetId), true, null);
    assertTrue(targetCodes(result).contains(source));
    assertEquals(Set.of(relationship), equivalences(result));
  }

  @Test
  void forwardTranslationReturnsEveryTargetOfAConceptInCodeOrder() {
    // An ambiguous concept is possibly equivalent to more than one concept, and every one of them
    // is a translation. The fixture writes the higher code first.
    final List<Translation> result =
        snomedService.translate(
            snomed(Rf2Mini.POSSIBLY_EQUIVALENT_TO_SOURCE),
            implicitConceptMap(Rf2Mini.POSSIBLY_EQUIVALENT_TO_REFSET),
            false,
            null);
    assertEquals(
        List.of(Rf2Mini.TYPE1_DIABETES, Rf2Mini.TYPE2_DIABETES), orderedTargetCodes(result));
  }

  @Test
  void associationReferenceSetOutsideTheImplicitConceptMapsTranslatesToNothing() {
    // WAS A is loaded with its association targets, as its membership shows, but it is not one of
    // the reference sets that THO defines as an implicit concept map, so it is unknown content in
    // both directions.
    final String wasAValueSet = Rf2Mini.SNOMED_URI + "?fhir_vs=refset/" + Rf2Mini.WAS_A_REFSET;
    assertTrue(snomedService.validateCode(wasAValueSet, snomed(Rf2Mini.WAS_A_SOURCE)));

    final String conceptMap = implicitConceptMap(Rf2Mini.WAS_A_REFSET);
    assertTrue(
        snomedService.translate(snomed(Rf2Mini.WAS_A_SOURCE), conceptMap, false, null).isEmpty());
    assertTrue(snomedService.translate(snomed(Rf2Mini.DIABETES), conceptMap, true, null).isEmpty());
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
}
