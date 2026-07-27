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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Designation;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.PropertyOrDesignation;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Service-level tests for {@code lookup} in local mode over the rf2-mini store: display selection
 * (default and acceptLanguage-driven), designations with their SNOMED use codings, the standard
 * SNOMED properties ({@code parent}, {@code child}, {@code inactive}, {@code moduleId}, {@code
 * effectiveTime}, {@code sufficientlyDefined}), attribute relationships as Coding properties,
 * inactive-concept resolvability, and the unknown-content and null-coding fallbacks.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceLookupTest {

  private static final String SYNONYM = "900000000000013009";
  private static final String FSN = "900000000000003001";
  private static final String PREFERRED_FOR_LANGUAGE = "preferredForLanguage";
  private static final String FINDING_SITE = "363698007";
  private static final String ASSOCIATED_MORPHOLOGY = "116676008";

  /** The GB English dialect named directly, as the extension form of its reference set. */
  private static final String GB_EXTENSION_TAG = "en-x-sctlang-90000000-00005080-04";

  private static TerminologyService service;

  @BeforeAll
  static void setUp() {
    service = serviceWith(null);
  }

  /**
   * Builds a service over the shared fixture store, optionally registering additional dialect
   * aliases.
   */
  @Nonnull
  private static TerminologyService serviceWith(
      @Nullable final Map<String, String> dialectAliases) {
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(
                LocalTerminologyConfiguration.builder()
                    .storagePath(LocalTerminologyFixture.storagePath())
                    .dialectAliases(dialectAliases)
                    .build())
            .build();
    return new LocalTerminologyService(configuration, Map.of());
  }

  private static Coding snomed(final String code) {
    return new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(code);
  }

  @Nonnull
  private static List<Type> propertyValues(
      @Nonnull final Coding coding, @Nonnull final String propertyCode) {
    return service.lookup(coding, propertyCode, null).stream()
        .filter(Property.class::isInstance)
        .map(Property.class::cast)
        .filter(p -> propertyCode.equals(p.getCode()))
        .map(Property::getValue)
        .toList();
  }

  @Nonnull
  private static List<Designation> designations(@Nonnull final Coding coding) {
    return service.lookup(coding, Designation.PROPERTY_CODE, null).stream()
        .filter(Designation.class::isInstance)
        .map(Designation.class::cast)
        .toList();
  }

  @Nullable
  private static String display(
      @Nonnull final Coding coding, @Nullable final String acceptLanguage) {
    return display(service, coding, acceptLanguage);
  }

  @Nullable
  private static String display(
      @Nonnull final TerminologyService over,
      @Nonnull final Coding coding,
      @Nullable final String acceptLanguage) {
    return over.lookup(coding, "display", acceptLanguage).stream()
        .filter(Property.class::isInstance)
        .map(Property.class::cast)
        .filter(p -> "display".equals(p.getCode()))
        .map(Property::getValueAsString)
        .findFirst()
        .orElse(null);
  }

  @Test
  void returnsPreferredDisplay() {
    assertEquals("Diabetes mellitus", display(snomed(Rf2Mini.DIABETES), null));
  }

  @Test
  void returnsPreferredDisplayForRequestedLanguage() {
    // The fixture carries English descriptions, so the English preferred term is returned.
    assertEquals("Diabetes mellitus", display(snomed(Rf2Mini.DIABETES), "en"));
  }

  // --- Dialect-aware display selection (User Story 1). ---

  @Test
  void returnsTheTermOfTheRequestedDialect() {
    // The three fixture concepts the two language reference sets disagree about. Asking for British
    // English and asking for American English return different terms, each the one its own
    // reference
    // set marks preferred.
    assertEquals(
        Rf2Mini.DIVERGENT_GB_ENDOCRINE, display(snomed(Rf2Mini.ENDOCRINE_STRUCTURE), "en-GB"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_ENDOCRINE, display(snomed(Rf2Mini.ENDOCRINE_STRUCTURE), "en-US"));
    assertEquals(
        Rf2Mini.DIVERGENT_GB_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-US"));
    assertEquals(
        Rf2Mini.DIVERGENT_GB_DEGENERATION, display(snomed(Rf2Mini.DEGENERATION_MORPH), "en-GB"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_DEGENERATION, display(snomed(Rf2Mini.DEGENERATION_MORPH), "en-US"));
  }

  @Test
  void returnsTheSameTermForBothDialectsWhereTheReferenceSetsAgree() {
    // Every other concept in the fixture has one term that both reference sets prefer.
    assertEquals("Diabetes mellitus", display(snomed(Rf2Mini.DIABETES), "en-GB"));
    assertEquals("Diabetes mellitus", display(snomed(Rf2Mini.DIABETES), "en-US"));
  }

  @Test
  void returnsTheSameTermWhenTheDialectIsNamedByItsReferenceSetIdentifier() {
    // A designation language reported on the way out can be requested on the way in, and answers
    // identically to the familiar tag it is equivalent to.
    assertEquals(
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB"),
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), GB_EXTENSION_TAG));
    assertEquals(
        Rf2Mini.DIVERGENT_GB_PANCREAS,
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), GB_EXTENSION_TAG));
  }

  @Test
  void fallsBackToTheStoredDisplayForAReferenceSetTheStoreLacks() {
    // The Spanish language reference set is not in this release, so the store's default display
    // answers. The store was imported from the International edition, whose default is US English.
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "es"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-AU"));
  }

  @Test
  void fallsBackToTheStoredDisplayForAReferenceSetThatPrefersNoSynonym() {
    // The fixture's simple reference set is present in the store but ranks no description, so a
    // dialect resolving to it can name no preferred synonym and the stored display answers.
    final TerminologyService aliased = serviceWith(Map.of("en-XX", Rf2Mini.SIMPLE_REFSET));
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS,
        display(aliased, snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-XX"));
  }

  @Test
  void fallsBackToTheStoredDisplayWhenNoDialectIsNamed() {
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), null));
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), ""));
    // A bare primary subtag names no single reference set, so it expresses no preference either.
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en"));
  }

  @Test
  void resolvesADialectRegisteredInTheConfiguration() {
    // A deployment can register a tag the built-in table does not carry.
    final TerminologyService aliased = serviceWith(Map.of("en-XX", Rf2Mini.GB_ENGLISH_REFSET));
    assertEquals(
        Rf2Mini.DIVERGENT_GB_PANCREAS,
        display(aliased, snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-XX"));
    // Without the alias the same tag resolves to nothing and the stored display answers.
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-XX"));
  }

  @Test
  void prefersAConfiguredDialectOverABuiltInOne() {
    // A configured entry for a built-in tag replaces it, so a deployment can correct one.
    final TerminologyService aliased = serviceWith(Map.of("en-US", Rf2Mini.GB_ENGLISH_REFSET));
    assertEquals(
        Rf2Mini.DIVERGENT_GB_PANCREAS,
        display(aliased, snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-US"));
  }

  @Test
  void leavesEveryOtherPropertyUntouchedByADialectRequest() {
    // Dialect selection affects the display and the designations only.
    for (final String property :
        List.of(
            "parent", "child", "inactive", "moduleId", "effectiveTime", "sufficientlyDefined")) {
      assertEquals(
          service.lookup(snomed(Rf2Mini.TYPE2_DIABETES), property, null),
          service.lookup(snomed(Rf2Mini.TYPE2_DIABETES), property, "en-GB"),
          "The " + property + " property differs under a dialect request");
    }
  }

  @Test
  void leavesAnAttributePropertysCodingDisplayAsTheStoredDisplay() {
    // The display carried by an attribute property's coding value is the target concept's stored
    // display, and is not dialect-qualified, matching the behaviour of the reference server. The
    // finding site of DIABETES is one of the divergent concepts, so a dialect-qualified request
    // would otherwise show through here.
    final List<Type> findingSites =
        service.lookup(snomed(Rf2Mini.DIABETES), FINDING_SITE, "en-GB").stream()
            .filter(Property.class::isInstance)
            .map(Property.class::cast)
            .map(Property::getValue)
            .toList();
    assertEquals(1, findingSites.size());
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, ((Coding) findingSites.get(0)).getDisplay());
  }

  // --- Weighted language preferences (User Story 3). ---

  @Test
  void takesTheTermOfTheHighestWeightedDialectTheStoreCanSatisfy() {
    assertEquals(
        Rf2Mini.DIVERGENT_GB_PANCREAS,
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB;q=0.9,en-US;q=0.5"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS,
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB;q=0.5,en-US;q=0.9"));
  }

  @Test
  void fallsToTheNextDialectInWeightOrderThatTheStoreCanSatisfy() {
    // The Spanish reference set is not in this release, so the lower-weighted GB English answers
    // rather than the stored display.
    assertEquals(
        Rf2Mini.DIVERGENT_GB_PANCREAS,
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "es;q=0.9,en-GB;q=0.5"));
  }

  @Test
  void neverUsesADialectGivenZeroWeight() {
    // Even as the only dialect the store could satisfy, a zero-weighted one is not used.
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB;q=0"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS,
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB;q=0,es;q=0.5"));
  }

  @Test
  void triesDialectsOfEqualWeightInTheOrderTheyWereWritten() {
    assertEquals(
        Rf2Mini.DIVERGENT_GB_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB,en-US"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-US,en-GB"));
  }

  @Test
  void readsAWildcardOrAnUnreadableListAsNoPreference() {
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "*"));
    assertEquals(
        Rf2Mini.DIVERGENT_US_PANCREAS,
        display(snomed(Rf2Mini.PANCREAS_STRUCTURE), "en-GB;q=notanumber"));
    assertEquals(Rf2Mini.DIVERGENT_US_PANCREAS, display(snomed(Rf2Mini.PANCREAS_STRUCTURE), ";;;"));
  }

  // --- Determinism of designations (User Story 4). ---

  @Test
  void returnsAnIdenticalDesignationListOnEveryCall() {
    // The list a caller sees is a sequence, so repeating the request must reproduce it exactly
    // rather
    // than merely returning the same set.
    assertEquals(
        designations(snomed(Rf2Mini.TYPE2_DIABETES)), designations(snomed(Rf2Mini.TYPE2_DIABETES)));
    assertEquals(designations(snomed(Rf2Mini.DIABETES)), designations(snomed(Rf2Mini.DIABETES)));
  }

  @Test
  void ordersTheDesignationsOfATermPreferredInSeveralReferenceSetsByReferenceSetIdentifier() {
    // "Diabetes mellitus" is the preferred synonym in both of the fixture's language reference
    // sets,
    // so it yields one preferredForLanguage designation per reference set. Which comes first is
    // otherwise decided by the order of a map, so it is fixed by reference set identifier: GB
    // English
    // (900000000000508004) before US English (900000000000509007).
    final List<String> dialectLanguages =
        designations(snomed(Rf2Mini.DIABETES)).stream()
            .filter(d -> d.getUse() != null && PREFERRED_FOR_LANGUAGE.equals(d.getUse().getCode()))
            .filter(d -> "Diabetes mellitus".equals(d.getValue()))
            .map(Designation::getLanguage)
            .filter(language -> language != null && language.contains("sctlang"))
            .toList();
    assertEquals(
        List.of("en-x-sctlang-90000000-00005080-04", "en-x-sctlang-90000000-00005090-07"),
        dialectLanguages);
  }

  @Test
  void resolvesInactiveConceptDisplay() {
    // Inactive concepts remain resolvable for lookup.
    assertEquals("Diabetes", display(snomed(Rf2Mini.DIABETES_INACTIVE), null));
  }

  @Test
  void returnsDesignationsWithSnomedUseCodings() {
    // "Diabetes mellitus" is the preferred synonym in the fixture's language reference set, so it
    // is designated preferredForLanguage rather than as a plain synonym, matching server
    // behaviour. It surfaces with both the dialect language code and the plain display language.
    final List<Designation> designations = designations(snomed(Rf2Mini.DIABETES));
    assertTrue(
        designations.stream()
            .anyMatch(
                d ->
                    d.getUse() != null
                        && PREFERRED_FOR_LANGUAGE.equals(d.getUse().getCode())
                        && "en-x-sctlang-90000000-00005090-07".equals(d.getLanguage())
                        && "Diabetes mellitus".equals(d.getValue())));
    assertTrue(
        designations.stream()
            .anyMatch(
                d ->
                    d.getUse() != null
                        && PREFERRED_FOR_LANGUAGE.equals(d.getUse().getCode())
                        && "en".equals(d.getLanguage())
                        && "Diabetes mellitus".equals(d.getValue())));
    assertTrue(
        designations.stream()
            .anyMatch(
                d ->
                    d.getUse() != null
                        && FSN.equals(d.getUse().getCode())
                        && "Diabetes mellitus (disorder)".equals(d.getValue())));
    // The preferred synonym must not also appear as a plain synonym designation.
    assertFalse(
        designations.stream()
            .anyMatch(
                d ->
                    d.getUse() != null
                        && SYNONYM.equals(d.getUse().getCode())
                        && "Diabetes mellitus".equals(d.getValue())));
  }

  @Test
  void returnsAcceptableSynonymDesignation() {
    // TYPE2_DIABETES carries an extra acceptable synonym "T2DM", designated as a plain synonym.
    final List<Designation> designations = designations(snomed(Rf2Mini.TYPE2_DIABETES));
    assertTrue(
        designations.stream()
            .anyMatch(
                d ->
                    d.getUse() != null
                        && SYNONYM.equals(d.getUse().getCode())
                        && "T2DM".equals(d.getValue())));
  }

  @Test
  void returnsParentProperty() {
    final List<Type> parents = propertyValues(snomed(Rf2Mini.TYPE2_DIABETES), "parent");
    assertEquals(1, parents.size());
    assertEquals(Rf2Mini.DIABETES, parents.get(0).primitiveValue());
    assertEquals("code", parents.get(0).fhirType());
  }

  @Test
  void returnsChildProperty() {
    final List<Type> children = propertyValues(snomed(Rf2Mini.TYPE2_DIABETES), "child");
    assertEquals(1, children.size());
    assertEquals(Rf2Mini.TYPE2_WITH_COMPLICATION, children.get(0).primitiveValue());
    assertEquals("code", children.get(0).fhirType());
  }

  @Test
  void returnsInactiveProperty() {
    assertEquals(
        "false", propertyValues(snomed(Rf2Mini.DIABETES), "inactive").get(0).primitiveValue());
    assertEquals(
        "true",
        propertyValues(snomed(Rf2Mini.DIABETES_INACTIVE), "inactive").get(0).primitiveValue());
  }

  @Test
  void returnsModuleIdProperty() {
    final List<Type> modules = propertyValues(snomed(Rf2Mini.DIABETES), "moduleId");
    assertEquals(1, modules.size());
    assertEquals(Rf2Mini.CORE_MODULE, modules.get(0).primitiveValue());
    assertEquals("code", modules.get(0).fhirType());
  }

  @Test
  void returnsEffectiveTimeProperty() {
    final List<Type> times = propertyValues(snomed(Rf2Mini.DIABETES), "effectiveTime");
    assertEquals(1, times.size());
    assertEquals("2023-06-01", times.get(0).primitiveValue());
    assertEquals("dateTime", times.get(0).fhirType());
  }

  @Test
  void returnsSufficientlyDefinedProperty() {
    // DIABETES is sufficiently defined; HYPERTENSION is primitive.
    assertEquals(
        "true",
        propertyValues(snomed(Rf2Mini.DIABETES), "sufficientlyDefined").get(0).primitiveValue());
    assertEquals(
        "false",
        propertyValues(snomed(Rf2Mini.HYPERTENSION), "sufficientlyDefined")
            .get(0)
            .primitiveValue());
  }

  @Test
  void returnsAttributeRelationshipsAsCodingProperties() {
    final List<Type> findingSites = propertyValues(snomed(Rf2Mini.DIABETES), FINDING_SITE);
    assertEquals(1, findingSites.size());
    assertEquals("Coding", findingSites.get(0).fhirType());
    assertEquals(Rf2Mini.PANCREAS_STRUCTURE, ((Coding) findingSites.get(0)).getCode());
    assertEquals(Rf2Mini.SNOMED_URI, ((Coding) findingSites.get(0)).getSystem());

    final List<Type> morphologies = propertyValues(snomed(Rf2Mini.DIABETES), ASSOCIATED_MORPHOLOGY);
    assertEquals(1, morphologies.size());
    assertEquals(Rf2Mini.DEGENERATION_MORPH, ((Coding) morphologies.get(0)).getCode());
  }

  @Test
  void returnsInactivePropertyForInactiveConcept() {
    // property_of on an inactive concept still resolves its content (inactive = true).
    assertEquals(
        "true",
        propertyValues(snomed(Rf2Mini.DIABETES_INACTIVE), "inactive").get(0).primitiveValue());
  }

  @Test
  void returnsEmptyForUnknownCode() {
    assertTrue(service.lookup(snomed("999999999"), "display", null).isEmpty());
  }

  @Test
  void returnsEmptyForUnknownSystem() {
    final Coding loinc = new Coding().setSystem("http://loinc.org").setCode("1234-5");
    assertTrue(service.lookup(loinc, "display", null).isEmpty());
  }

  @Test
  void returnsEmptyForNullSystemOrCode() {
    assertTrue(service.lookup(new Coding().setCode(Rf2Mini.DIABETES), "display", null).isEmpty());
    assertTrue(
        service.lookup(new Coding().setSystem(Rf2Mini.SNOMED_URI), "display", null).isEmpty());
  }

  @Test
  void filtersToTheRequestedPropertyCode() {
    // Requesting a single property returns only matching properties, not the whole concept.
    final List<PropertyOrDesignation> result =
        service.lookup(snomed(Rf2Mini.DIABETES), "inactive", null);
    assertFalse(result.isEmpty());
    assertTrue(
        result.stream()
            .filter(Property.class::isInstance)
            .map(Property.class::cast)
            .allMatch(p -> "inactive".equals(p.getCode())));
  }
}
