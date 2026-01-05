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

package au.csiro.pathling.terminology.mock;

import static java.util.Objects.isNull;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;

import au.csiro.pathling.terminology.TerminologyService;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.jetbrains.annotations.Contract;

public class MockTerminologyService implements TerminologyService {

  public static final String SNOMED_URI = "http://snomed.info/sct";
  public static final String LOINC_URI = "http://loinc.org";
  public static final String BETA_2_GLOBULIN_CODE = "55915-3";
  public static final String FR_LANG_CODE = "fr-FR";

  record SystemAndCode(@Nonnull String system, @Nonnull String code) {

    @Nonnull
    @Contract("_ -> new")
    static SystemAndCode of(@Nonnull final Coding coding) {
      return new SystemAndCode(coding.getSystem(), coding.getCode());
    }
  }

  static class ConceptMap {

    public static final ConceptMap EMPTY =
        new ConceptMap(Collections.emptyMap(), Collections.emptyMap());

    private final Map<SystemAndCode, List<Translation>> mappings;
    private final Map<SystemAndCode, List<Translation>> invertedMappings;

    ConceptMap(
        final Map<SystemAndCode, List<Translation>> mappings,
        final Map<SystemAndCode, List<Translation>> invertedMappings) {
      this.mappings = mappings;
      this.invertedMappings = invertedMappings;
    }

    public List<Translation> translate(
        @Nonnull final Coding coding, final boolean reverse, @Nullable final String target) {
      return (reverse ? invertedMappings : mappings)
          .getOrDefault(SystemAndCode.of(coding), Collections.emptyList()).stream()
              .filter(c -> (isNull(target) || target.equals(c.getConcept().getSystem())))
              .toList();
    }
  }

  static class ValueSet {

    private final Set<SystemAndCode> members;

    ValueSet(final Coding... coding) {
      members = Stream.of(coding).map(SystemAndCode::of).collect(Collectors.toUnmodifiableSet());
    }

    boolean contains(@Nonnull final Coding coding) {
      return members.contains(SystemAndCode.of(coding));
    }

    public static final ValueSet EMPTY = new ValueSet();
  }

  private final Map<String, ValueSet> valueSets = new HashMap<>();
  private final Map<String, ConceptMap> conceptMap = new HashMap<>();
  private final Set<Pair<SystemAndCode, SystemAndCode>> subsumes = new HashSet<>();

  public MockTerminologyService() {
    valueSets.put(
        "http://snomed.info/sct?fhir_vs=refset/723264001",
        new ValueSet(new Coding(SNOMED_URI, "368529001", null)));
    valueSets.put(
        "http://loinc.org/vs/LP14885-5",
        new ValueSet(new Coding(LOINC_URI, BETA_2_GLOBULIN_CODE, null)));

    subsumes.add(
        Pair.of(
            new SystemAndCode(SNOMED_URI, "107963000"), new SystemAndCode(SNOMED_URI, "63816008")));

    conceptMap.put(
        "http://snomed.info/sct?fhir_cm=100",
        new ConceptMap(
            ImmutableMap.of(
                new SystemAndCode(SNOMED_URI, "368529001"),
                List.of(
                    Translation.of(EQUIVALENT, new Coding(SNOMED_URI, "368529002", null)),
                    Translation.of(RELATEDTO, new Coding(LOINC_URI, "55916-3", null)))),
            Collections.emptyMap()));

    conceptMap.put(
        "http://snomed.info/sct?fhir_cm=200",
        new ConceptMap(
            Collections.emptyMap(),
            ImmutableMap.of(
                new SystemAndCode(LOINC_URI, BETA_2_GLOBULIN_CODE),
                List.of(
                    Translation.of(RELATEDTO, new Coding(SNOMED_URI, "368529002", null)),
                    Translation.of(EQUIVALENT, new Coding(LOINC_URI, "55916-3", null))))));
  }

  @Override
  public boolean validateCode(@Nonnull final String codeSystemUrl, @Nonnull final Coding coding) {
    return valueSets.getOrDefault(codeSystemUrl, ValueSet.EMPTY).contains(coding);
  }

  @Nonnull
  @Override
  public List<Translation> translate(
      @Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse,
      @Nullable final String target) {

    return conceptMap
        .getOrDefault(conceptMapUrl, ConceptMap.EMPTY)
        .translate(coding, reverse, target);
  }

  @Override
  @Nonnull
  public ConceptSubsumptionOutcome subsumes(
      @Nonnull final Coding codingA, @Nonnull final Coding codingB) {

    final SystemAndCode systemAndCodeA = SystemAndCode.of(codingA);
    final SystemAndCode systemAndCodeB = SystemAndCode.of(codingB);

    if (systemAndCodeA.equals(systemAndCodeB)) {
      return ConceptSubsumptionOutcome.EQUIVALENT;
    } else if (subsumes.contains(Pair.of(systemAndCodeA, systemAndCodeB))) {
      return ConceptSubsumptionOutcome.SUBSUMES;
    } else if (subsumes.contains(Pair.of(systemAndCodeB, systemAndCodeA))) {
      return ConceptSubsumptionOutcome.SUBSUMEDBY;
    } else {
      return ConceptSubsumptionOutcome.NOTSUBSUMED;
    }
  }

  @Nonnull
  @Override
  public List<PropertyOrDesignation> lookup(
      @Nonnull final Coding coding,
      @Nullable final String propertyCode,
      @Nullable final String acceptLanguage) {

    final Coding snomedCoding = new Coding(SNOMED_URI, "439319006", null);

    final Coding loincCoding = new Coding(LOINC_URI, BETA_2_GLOBULIN_CODE, null);

    final Map<String, String> loincCodingDisplayNames =
        Map.of(
            "en",
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
            FR_LANG_CODE,
            "Bêta-2 globulines [Masse/Volume] Liquide céphalorachidien",
            "de",
            "Beta-2-Globulin [Masse/Volumen] in Zerebrospinalflüssigkeit mit Elektrophorese");

    final Coding useDisplay =
        new Coding("http://terminology.hl7.org/CodeSystem/designation-usage", "display", null);

    final Coding useFullySpecifiedName =
        new Coding(SNOMED_URI, "900000000000003001", "Fully specified name");

    if (SystemAndCode.of(snomedCoding).equals(SystemAndCode.of(coding))) {
      return List.of(
          Property.of("parent", new CodeType("785673007")),
          Property.of("parent", new CodeType("74754006")),
          Designation.of(useDisplay, "en", "Screening for phenothiazine in serum"),
          Designation.of(
              useFullySpecifiedName, "en", "Screening for phenothiazine in serum (procedure)"));
    } else if (SystemAndCode.of(loincCoding).equals(SystemAndCode.of(coding))) {
      return List.of(
          Property.of(
              "display",
              new StringType(
                  loincCodingDisplayNames.get(acceptLanguage != null ? acceptLanguage : "en"))),
          Property.of("inactive", new BooleanType(false)),
          Designation.of(useDisplay, "en", loincCodingDisplayNames.get("en")),
          Designation.of(useDisplay, FR_LANG_CODE, loincCodingDisplayNames.get(FR_LANG_CODE)),
          Designation.of(
              useFullySpecifiedName,
              FR_LANG_CODE,
              "Beta 2 globulin:MCnc:Pt:CSF:Qn:Electrophoresis"));
    } else {
      return Collections.emptyList();
    }
  }
}
