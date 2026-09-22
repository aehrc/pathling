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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.expand.ExpansionLimitExceededException;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetExpansionException;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.test.FhirFixtures;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Service-level tests for value set expansion in local mode: SNOMED CT implicit value sets over the
 * rf2-mini store, explicit value sets over the animal-species store, pinned versions, the member
 * limit, and supplied resources carrying a compose or an expansion.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceExpandTest {

  private static final int NO_LIMIT = Integer.MAX_VALUE;
  private static final String ISA_DIABETES =
      Rf2Mini.SNOMED_URI + "?fhir_vs=isa/" + Rf2Mini.DIABETES;
  private static final String INACTIVE_ONLY =
      "http://fhir.org/VCL?v1="
          + URLEncoder.encode(
              "(" + Rf2Mini.SNOMED_URI + ")inactive = true", StandardCharsets.UTF_8);
  private static final String ANIMAL_SPECIES_VERSION =
      FhirFixtures.ANIMAL_SPECIES + "|" + FhirFixtures.VERSION;

  private static TerminologyService snomedService;
  private static TerminologyService fhirService;
  private static ConceptDictionary dictionary;

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
    snomedService = new LocalTerminologyService(configuration, Map.of());
    fhirService = FhirTerminologyFixture.service();
    dictionary = LocalTerminologyFixture.indexes().dictionary();
  }

  @Nonnull
  private static ValueSetMember memberWithCode(
      @Nonnull final ValueSetExpansion expansion, @Nonnull final String code) {
    return expansion.getMembers().stream()
        .filter(member -> code.equals(member.getCode()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No member with code " + code));
  }

  @Nonnull
  private static List<String> codes(@Nonnull final ValueSetExpansion expansion) {
    return expansion.getMembers().stream().map(ValueSetMember::getCode).toList();
  }

  @Nonnull
  private static ValueSet valueSet(@Nonnull final String url) {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(url);
    return valueSet;
  }

  @Test
  void expandsSnomedImplicitValueSet() {
    final ValueSetExpansion expansion =
        snomedService.expand(ISA_DIABETES, null, NO_LIMIT).orElseThrow();

    assertEquals(ISA_DIABETES, expansion.getUrl());
    assertNull(expansion.getVersion());
    assertNull(expansion.getIdentifier());
    assertNull(expansion.getTimestamp());
    assertEquals(
        List.of(Rf2Mini.SNOMED_URI + "|" + Rf2Mini.VERSION_20230601),
        expansion.getCodeSystemVersions());
    final List<String> codes = codes(expansion);
    assertTrue(codes.contains(Rf2Mini.DIABETES));
    assertTrue(codes.contains(Rf2Mini.TYPE1_DIABETES));
    assertTrue(codes.contains(Rf2Mini.TYPE2_WITH_COMPLICATION));
    assertFalse(codes.contains(Rf2Mini.HYPERTENSION));
    // The inactive concept is not a member of an implicit value set that does not ask for it.
    assertFalse(codes.contains(Rf2Mini.DIABETES_INACTIVE));
    // Members are ordered by code, not by the store's internal identifiers.
    assertEquals(codes.stream().sorted().toList(), codes);
    for (final ValueSetMember member : expansion.getMembers()) {
      assertEquals(Rf2Mini.SNOMED_URI, member.getSystem());
      assertEquals(Rf2Mini.VERSION_20230601, member.getVersion());
      assertNull(member.getInactive());
    }
    final ValueSetMember diabetes = memberWithCode(expansion, Rf2Mini.DIABETES);
    assertEquals(dictionary.display(dictionary.denseId(Rf2Mini.DIABETES)), diabetes.getDisplay());
  }

  @Test
  void marksInactiveConceptInactive() {
    final ValueSetExpansion expansion =
        snomedService.expand(INACTIVE_ONLY, null, NO_LIMIT).orElseThrow();

    assertEquals(List.of(Rf2Mini.DIABETES_INACTIVE), codes(expansion));
    final ValueSetMember inactive = memberWithCode(expansion, Rf2Mini.DIABETES_INACTIVE);
    assertEquals(Boolean.TRUE, inactive.getInactive());
    assertEquals(Rf2Mini.VERSION_20230601, inactive.getVersion());
    assertEquals(
        dictionary.display(dictionary.denseId(Rf2Mini.DIABETES_INACTIVE)), inactive.getDisplay());
  }

  @Test
  void expandsExplicitValueSetByCanonical() {
    final ValueSetExpansion expansion =
        fhirService.expand(FhirFixtures.VS_MAMMALS_ENUMERATED, null, NO_LIMIT).orElseThrow();

    assertEquals(FhirFixtures.VS_MAMMALS_ENUMERATED, expansion.getUrl());
    assertEquals(List.of(ANIMAL_SPECIES_VERSION), expansion.getCodeSystemVersions());
    assertEquals(
        List.of(
            new ValueSetMember(
                FhirFixtures.ANIMAL_SPECIES, FhirFixtures.VERSION, FhirFixtures.CAT, "Cat", null),
            new ValueSetMember(
                FhirFixtures.ANIMAL_SPECIES, FhirFixtures.VERSION, FhirFixtures.DOG, "Dog", null),
            new ValueSetMember(
                FhirFixtures.ANIMAL_SPECIES,
                FhirFixtures.VERSION,
                FhirFixtures.WHALE,
                "Whale",
                null)),
        expansion.getMembers());
  }

  @Test
  void expandsExplicitValueSetPinnedToVersion() {
    final ValueSetExpansion expansion =
        fhirService
            .expand(FhirFixtures.VS_MAMMALS_ENUMERATED, FhirFixtures.VERSION, NO_LIMIT)
            .orElseThrow();

    assertEquals(FhirFixtures.VERSION, expansion.getVersion());
    assertEquals(List.of(FhirFixtures.CAT, FhirFixtures.DOG, FhirFixtures.WHALE), codes(expansion));
  }

  @Test
  void returnsEmptyForUnknownPinnedVersion() {
    assertTrue(fhirService.expand(FhirFixtures.VS_MAMMALS_ENUMERATED, "9.9.9", NO_LIMIT).isEmpty());
  }

  @Test
  void rejectsPinnedVersionOnImplicitUrl() {
    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class,
            () -> snomedService.expand(ISA_DIABETES, "20230601", NO_LIMIT));

    assertTrue(e.getMessage().contains("version"), e.getMessage());
  }

  @Test
  void returnsEmptyForUnknownUrl() {
    assertTrue(snomedService.expand("http://loinc.org/vs/LP14885-5", null, NO_LIMIT).isEmpty());
    assertTrue(
        fhirService.expand("http://example.org/fhir/ValueSet/missing", null, NO_LIMIT).isEmpty());
  }

  @Test
  void rejectsMembershipOneOverTheLimit() {
    final int cardinality =
        snomedService.expand(ISA_DIABETES, null, NO_LIMIT).orElseThrow().getMembers().size();

    final ExpansionLimitExceededException e =
        assertThrows(
            ExpansionLimitExceededException.class,
            () -> snomedService.expand(ISA_DIABETES, null, cardinality - 1));

    assertEquals(cardinality - 1, e.getLimit());
    assertEquals(
        cardinality,
        snomedService.expand(ISA_DIABETES, null, cardinality).orElseThrow().getMembers().size());
  }

  @Test
  void evaluatesSuppliedCompose() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    supplied.setVersion("1");
    final ConceptSetComponent include = supplied.getCompose().addInclude();
    include.setSystem(Rf2Mini.SNOMED_URI);
    include.addConcept().setCode(Rf2Mini.TYPE2_DIABETES);
    include.addConcept().setCode(Rf2Mini.DIABETES);

    final ValueSetExpansion expansion = snomedService.expand(supplied, NO_LIMIT);

    assertEquals("http://example.org/ValueSet/supplied", expansion.getUrl());
    assertEquals("1", expansion.getVersion());
    assertEquals(List.of(Rf2Mini.DIABETES, Rf2Mini.TYPE2_DIABETES), codes(expansion));
    assertEquals(
        List.of(Rf2Mini.SNOMED_URI + "|" + Rf2Mini.VERSION_20230601),
        expansion.getCodeSystemVersions());
    assertEquals(
        dictionary.display(dictionary.denseId(Rf2Mini.DIABETES)),
        memberWithCode(expansion, Rf2Mini.DIABETES).getDisplay());
  }

  @Test
  void evaluatesSuppliedComposeWithFilter() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    supplied
        .getCompose()
        .addInclude()
        .setSystem(Rf2Mini.SNOMED_URI)
        .addFilter()
        .setProperty("concept")
        .setOp(ValueSet.FilterOperator.ISA)
        .setValue(Rf2Mini.TYPE2_DIABETES);

    final ValueSetExpansion expansion = snomedService.expand(supplied, NO_LIMIT);

    assertEquals(
        List.of(Rf2Mini.TYPE2_DIABETES, Rf2Mini.TYPE2_WITH_COMPLICATION), codes(expansion));
  }

  @Test
  void evaluatesSuppliedComposeWithNestedReference() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    supplied.getCompose().addInclude().addValueSet(FhirFixtures.VS_MAMMALS_ENUMERATED);

    final ValueSetExpansion expansion = fhirService.expand(supplied, NO_LIMIT);

    assertEquals(List.of(FhirFixtures.CAT, FhirFixtures.DOG, FhirFixtures.WHALE), codes(expansion));
  }

  @Test
  void flattensSuppliedExpansionWithoutEvaluation() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    supplied.getCompose().addInclude().setSystem(Rf2Mini.SNOMED_URI).addConcept().setCode("1");
    supplied.getExpansion().addContains().setSystem("http://loinc.org").setCode("1234-5");

    final ValueSetExpansion expansion = snomedService.expand(supplied, NO_LIMIT);

    assertEquals(
        List.of(new ValueSetMember("http://loinc.org", null, "1234-5", null, null)),
        expansion.getMembers());
  }

  @Test
  void rejectsComposeOverCodeSystemNotInStore() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    supplied.getCompose().addInclude().setSystem("http://loinc.org").addConcept().setCode("1234-5");

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class, () -> snomedService.expand(supplied, NO_LIMIT));

    assertTrue(e.getMessage().contains("http://loinc.org"), e.getMessage());
  }

  @Test
  void rejectsComposeSpanningSeveralCodeSystems() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    supplied.getCompose().addInclude().setSystem(Rf2Mini.SNOMED_URI).addConcept().setCode("1");
    supplied.getCompose().addInclude().setSystem("http://loinc.org").addConcept().setCode("1234-5");

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class, () -> snomedService.expand(supplied, NO_LIMIT));

    assertTrue(e.getMessage().contains("several code systems"), e.getMessage());
  }

  @Test
  void rejectsResourceWithNeitherExpansionNorCompose() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class, () -> snomedService.expand(supplied, NO_LIMIT));

    assertTrue(e.getMessage().contains("neither"), e.getMessage());
  }

  @Test
  void rejectsSuppliedComposeOverTheLimit() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    final ConceptSetComponent include = supplied.getCompose().addInclude();
    include.setSystem(Rf2Mini.SNOMED_URI);
    include.addConcept().setCode(Rf2Mini.TYPE2_DIABETES);
    include.addConcept().setCode(Rf2Mini.DIABETES);

    assertThrows(ExpansionLimitExceededException.class, () -> snomedService.expand(supplied, 1));
  }

  @Test
  void expandsEmptyMembershipWithoutError() {
    final ValueSet supplied = valueSet("http://example.org/ValueSet/supplied");
    supplied.getCompose().addInclude().setSystem(Rf2Mini.SNOMED_URI).addConcept().setCode("0");

    final Optional<ValueSetExpansion> byCanonical =
        snomedService.expand(Rf2Mini.SNOMED_URI + "?fhir_vs=isa/0", null, NO_LIMIT);

    assertTrue(byCanonical.isPresent());
    assertTrue(byCanonical.get().getMembers().isEmpty());
    assertTrue(snomedService.expand(supplied, NO_LIMIT).getMembers().isEmpty());
  }
}
