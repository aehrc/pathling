/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.helpers;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.Relation;
import au.csiro.pathling.test.fixtures.RelationBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.ValueSet;

public final class TerminologyHelpers {

  private TerminologyHelpers() {
  }

  public static final String SNOMED_URI = "http://snomed.info/sct";
  public static final String SNOMED_VERSION = "http://snomed.info/sct/32506021000036107/version/20210331";

  private static final String AST_URI = "http://csiro.au/fhir/au-states-territories";

  public static final String CM_HIST_ASSOCIATIONS = "http://snomed.info/sct?fhir_cm=900000000000526001";

  public static final List<ConceptMapEquivalence> ALL_EQUIVALENCES = Arrays
      .asList(ConceptMapEquivalence.values());

  public static final Coding CD_SNOMED_720471000168102 = snomedCoding("720471000168102",
      "Duodopa intestinal gel");
  public static final Coding CD_SNOMED_72940011000036107 = snomedCoding("72940011000036107",
      "Duodopa gel: intestinal");


  public static final Coding CD_SNOMED_403190006 = snomedCoding("403190006",
      "Epidermal burn of skin");

  public static final Coding CD_SNOMED_284551006 = snomedCoding("284551006",
      "Laceration of foot");

  @SuppressWarnings("unused")
  public static Coding CD_SNOMED_63816008 = snomedCoding("63816008",
      "Left hepatectomy");

  // Subsumes CD_SNOMED_63816008
  public static final Coding CD_SNOMED_107963000 = snomedCoding("107963000",
      "Liver resection");

  public static final Coding CD_SNOMED_VER_63816008 = snomedVersionedCoding("63816008",
      "Left hepatectomy");

  // Subsumes CD_SNOMED_63816008
  public static final Coding CD_SNOMED_VER_107963000 = snomedVersionedCoding("107963000",
      "Liver resection");

  public static final Coding CD_SNOMED_VER_403190006 = snomedVersionedCoding("403190006",
      "Epidermal burn of skin");

  public static final Coding CD_SNOMED_VER_284551006 = snomedVersionedCoding("284551006",
      "Laceration of foot");


  public static final Coding CD_AST_VIC = new Coding(AST_URI, "VIC", "Victoria");

  // http://snomed.info/sct|444814009 -- subsumes --> http://snomed.info/sct|40055000


  public static final Coding CD_SNOMED_VER_40055000 = snomedVersionedCoding("40055000",
      "Chronic sinusitis (disorder)");
  private static final Coding CD_SNOMED_VER_444814009 = snomedVersionedCoding("444814009",
      "Viral sinusitis (disorder)");

  public static final Relation REL_SNOMED_444814009_SUBSUMES_40055000 = RelationBuilder.empty()
      .add(CD_SNOMED_VER_444814009, CD_SNOMED_VER_40055000).build();

  @Nonnull
  public static SimpleCoding snomedSimple(@Nonnull final String code) {
    return new SimpleCoding(SNOMED_URI, code);
  }

  @Nonnull
  public static SimpleCoding testSimple(@Nonnull final String code) {
    return new SimpleCoding(SNOMED_URI, code);
  }

  @Nonnull
  public static SimpleCoding simpleOf(@Nonnull final Coding code) {
    return new SimpleCoding(code);
  }

  @Nonnull
  public static Set<SimpleCoding> setOfSimpleFrom(@Nonnull final Coding... codings) {
    return Arrays.stream(codings).map(TerminologyHelpers::simpleOf)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Nonnull
  public static Set<SimpleCoding> setOfSimpleFrom(@Nonnull final CodeableConcept... validMembers) {
    return Arrays.stream(validMembers)
        .flatMap(codeableConcept -> codeableConcept.getCoding().stream())
        .map(TerminologyHelpers::simpleOf)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Nonnull
  public static Set<SimpleCoding> setOfSimpleFrom(@Nonnull final ValueSet valueSet) {
    return valueSet.getExpansion().getContains().stream()
        .map(contains -> new SimpleCoding(contains.getSystem(), contains.getCode(),
            contains.getVersion()))
        .collect(Collectors.toSet());
  }

  @Nonnull
  public static Coding newVersionedCoding(@Nonnull final String system, @Nonnull final String code,
      @Nonnull final String version, @Nonnull final String displayName) {
    return new Coding(system, code, displayName).setVersion(version);
  }

  @Nonnull
  private static Coding snomedCoding(@Nonnull final String code,
      @Nonnull final String displayName) {
    return new Coding(SNOMED_URI, code, displayName);
  }

  @Nonnull
  private static Coding snomedVersionedCoding(@Nonnull final String code,
      @Nonnull final String displayName) {
    return newVersionedCoding(SNOMED_URI, code, SNOMED_VERSION, displayName);
  }


}
