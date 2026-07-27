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

  private static TerminologyService service;

  @BeforeAll
  static void setUp() {
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(
                LocalTerminologyConfiguration.builder()
                    .storagePath(LocalTerminologyFixture.storagePath())
                    .build())
            .build();
    service = new LocalTerminologyService(configuration, Map.of());
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
    return service.lookup(coding, "display", acceptLanguage).stream()
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
  void ordersAttributeRelationshipPropertiesByTypeCode() {
    // The relationship index is keyed by type code in a hash map. Two codes that share a bucket
    // iterate in the order they were inserted, which is the order their rows were read from the
    // store, so emitting the types in that order would let the store's physical layout show
    // through in a result a caller can see. The fixture's two attribute types collide, which is
    // what makes this assertion load-bearing.
    final List<String> attributeTypes =
        service.lookup(snomed(Rf2Mini.DIABETES), null, null).stream()
            .filter(Property.class::isInstance)
            .map(Property.class::cast)
            .map(Property::getCode)
            .filter(code -> FINDING_SITE.equals(code) || ASSOCIATED_MORPHOLOGY.equals(code))
            .toList();
    assertEquals(List.of(ASSOCIATED_MORPHOLOGY, FINDING_SITE), attributeTypes);
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
