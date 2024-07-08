/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.helpers;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

public final class TerminologyHelpers {

  private TerminologyHelpers() {
  }

  public static final String SNOMED_URI = "http://snomed.info/sct";
  public static final String SNOMED_VERSION = "http://snomed.info/sct/32506021000036107/version/20210331";
  public static final String SNOMED_VERSION_20220930 = "http://snomed.info/sct/32506021000036107/version/20220930";

  public static final String AUTOMAP_INPUT_URI = "http://ontoserver.csiro.au/fhir/CodeSystem/codesystem-terms";

  private static final String AST_URI = "http://csiro.au/fhir/au-states-territories";

  public static final String CM_HIST_ASSOCIATIONS = "http://snomed.info/sct?fhir_cm=900000000000526001";

  public static final String CM_AUTOMAP_DEFAULT = "http://ontoserver.csiro.au/fhir/ConceptMap/automapstrategy-default";

  public static final List<ConceptMapEquivalence> ALL_EQUIVALENCES = Arrays
      .asList(ConceptMapEquivalence.values());

  public static final List<ConceptMapEquivalence> INEXACT = List.of(ConceptMapEquivalence.INEXACT);

  public static final Coding CD_SNOMED_720471000168102 = snomedCoding("720471000168102",
      "Duodopa intestinal gel");


  public static final Coding CD_SNOMED_720471000168102_VER2021 = snomedVersionedCoding(
      "720471000168102",
      "Duodopa intestinal gel");


  public static final Coding CD_SNOMED_72940011000036107 = snomedCoding("72940011000036107",
      "Duodopa gel: intestinal");


  public static final Coding CD_SNOMED_403190006 = snomedCoding("403190006",
      "First degree burn");

  public static final Coding CD_SNOMED_284551006 = snomedCoding("284551006",
      "Laceration of foot");

  public static final Coding CD_SNOMED_2121000032108 = snomedCoding("2121000032108",
      "Laparoscopic-assisted partial hepatectomy");

  public static final Coding CD_SNOMED_444814009 = snomedCoding("444814009",
      "Viral sinusitis (disorder)");

  public static final Coding CD_SNOMED_195662009 = snomedCoding("195662009",
      "Acute viral pharyngitis (disorder)");

  public static final Coding CD_SNOMED_40055000 = snomedCoding("40055000",
      "Chronic sinusitis (disorder)");

  @SuppressWarnings("unused")
  public static Coding CD_SNOMED_63816008 = snomedCoding("63816008",
      "Left hepatectomy");


  // Subsumes CD_SNOMED_63816008
  public static final Coding CD_SNOMED_107963000 = snomedCoding("107963000",
      "Liver resection");

  public static final Coding CD_SNOMED_VER_63816008 = snomedVersionedCoding("63816008",
      "Left hepatectomy");

  public static final Coding CD_SNOMED_63816008_VER2022 = newVersionedCoding(SNOMED_URI, "63816008",
      SNOMED_VERSION_20220930, "Left hepatectomy");

  // Subsumes CD_SNOMED_63816008
  public static final Coding CD_SNOMED_VER_107963000 = snomedVersionedCoding("107963000",
      "Liver resection");

  public static final Coding CD_SNOMED_VER_403190006 = snomedVersionedCoding("403190006",
      "Epidermal burn of skin");

  public static final Coding CD_SNOMED_VER_284551006 = snomedVersionedCoding("284551006",
      "Laceration of foot");


  public static final Coding CD_SNOMED_900000000000003001 = snomedCoding("900000000000003001",
      "Fully specified name");


  public static final Coding CD_AST_VIC = new Coding(AST_URI, "VIC", "Victoria");

  // http://snomed.info/sct|444814009 -- subsumes --> http://snomed.info/sct|40055000

  public static final Coding CD_SNOMED_VER_40055000 = snomedVersionedCoding("40055000",
      "Chronic sinusitis (disorder)");
  private static final Coding CD_SNOMED_VER_444814009 = snomedVersionedCoding("444814009",
      "Viral sinusitis (disorder)");

  public static final Coding CD_AUTOMAP_INPUT_DESCRIPTION_ID = new Coding(AUTOMAP_INPUT_URI,
      "101013", null);

  // LOINC

  public static final String LOINC_URI = "http://loinc.org";
  public static final String LOINC_NAME = "LOINC v2.73";
  public static final Coding LC_55915_3 = new Coding(LOINC_URI, "55915-3",
      "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis");
  public static final Coding LC_29463_7 = new Coding(LOINC_URI, "29463-7",
      "Body weight");

  // Others

  public static final Coding HL7_USE_DISPLAY = new Coding(
      "http://terminology.hl7.org/CodeSystem/designation-usage",
      "display", null);

  @Nonnull
  public static Coding newVersionedCoding(@Nonnull final String system, @Nonnull final String code,
      @Nonnull final String version, @Nonnull final String displayName) {
    return new Coding(system, code, displayName).setVersion(version);
  }

  @Nonnull
  public static Coding snomedCoding(@Nonnull final String code,
      @Nonnull final String displayName) {
    return new Coding(SNOMED_URI, code, displayName);
  }

  @Nonnull
  public static Coding snomedCoding(@Nonnull final String code,
      @Nonnull final String displayName, @Nonnull final String version) {
    final Coding coding = snomedCoding(code, displayName);
    coding.setVersion(version);
    return coding;
  }

  @Nonnull
  public static Coding snomedVersionedCoding(@Nonnull final String code,
      @Nonnull final String displayName) {
    return newVersionedCoding(SNOMED_URI, code, SNOMED_VERSION, displayName);
  }


  @Nonnull
  public static Coding mockCoding(@Nonnull final String system, @Nonnull final String code,
      final int index) {
    return new Coding(system, code + "-" + index, "Display-" + index);
  }

}
