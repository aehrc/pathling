/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.terminology.TerminologyService2;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;

public class MockTerminologyService2 implements TerminologyService2 {

  @Value
  @AllArgsConstructor
  static class SystemAndCode {

    @Nonnull
    String system;
    @Nonnull
    String code;

    static SystemAndCode of(@Nonnull final Coding coding) {
      return new SystemAndCode(coding.getSystem(), coding.getCode());
    }
  }

  static class ConceptMap {

    public static final ConceptMap EMPTY = new ConceptMap(Collections.emptyMap(),
        Collections.emptyMap());

    private final Map<SystemAndCode, List<Translation>> mappings;
    private final Map<SystemAndCode, List<Translation>> invertedMappings;

    ConceptMap(final Map<SystemAndCode, List<Translation>> mappings,
        final Map<SystemAndCode, List<Translation>> invertedMappings
    ) {
      this.mappings = mappings;
      this.invertedMappings = invertedMappings;
    }

    public List<Translation> translate(Coding coding, boolean reverse, final String target) {
      return (reverse
              ? invertedMappings
              : mappings)
          .getOrDefault(SystemAndCode.of(coding), Collections.emptyList()).stream()
          .filter(c -> (isNull(target) || target.equals(c.getConcept().getSystem())))
          .collect(Collectors.toUnmodifiableList());
    }
  }

  static class ValueSet {

    private final Set<SystemAndCode> members;

    ValueSet(final Coding... coding) {
      members = Stream.of(coding)
          .map(SystemAndCode::of)
          .collect(Collectors.toUnmodifiableSet());
    }

    boolean contains(@Nonnull final Coding coding) {
      return members.contains(SystemAndCode.of(coding));
    }

    public static final ValueSet EMPTY = new ValueSet();
  }

  private final Map<String, ValueSet> valueSets = new HashMap<>();
  private final Map<String, ConceptMap> conceptMap = new HashMap<>();
  private final Set<Pair<SystemAndCode, SystemAndCode>> subsumes = new HashSet<>();

  public MockTerminologyService2() {
    valueSets.put("http://snomed.info/sct?fhir_vs=refset/723264001",
        new ValueSet(new Coding("http://snomed.info/sct", "368529001", null)));
    valueSets.put("http://loinc.org/vs/LP14885-5",
        new ValueSet(new Coding("http://loinc.org", "55915-3", null)));

    subsumes.add(Pair.of(new SystemAndCode("http://snomed.info/sct", "107963000"),
        new SystemAndCode("http://snomed.info/sct", "63816008")));

    conceptMap.put("http://snomed.info/sct?fhir_cm=100", new ConceptMap(
        ImmutableMap.of(
            new SystemAndCode("http://snomed.info/sct", "368529001"), List.of(
                Translation.of(EQUIVALENT,
                    new Coding("http://snomed.info/sct", "368529002", null)),
                Translation.of(RELATEDTO, new Coding("http://loinc.org", "55916-3", null))
            )
        ), Collections.emptyMap()));

    conceptMap.put("http://snomed.info/sct?fhir_cm=200", new ConceptMap(
        Collections.emptyMap(),
        ImmutableMap.of(
            new SystemAndCode("http://loinc.org", "55915-3"), List.of(
                Translation.of(RELATEDTO,
                    new Coding("http://snomed.info/sct", "368529002", null)),
                Translation.of(EQUIVALENT, new Coding("http://loinc.org", "55916-3", null))
            )
        ))
    );

  }

  @Override
  public boolean validate(@Nonnull final String url, @Nonnull final Coding coding) {
    return valueSets.getOrDefault(url, ValueSet.EMPTY).contains(coding);
  }

  @Nonnull
  @Override
  public List<Translation> translate(@Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse, @Nullable final String target) {

    return conceptMap.getOrDefault(conceptMapUrl, ConceptMap.EMPTY)
        .translate(coding, reverse, target);

  }

  @Override
  @Nonnull
  public ConceptSubsumptionOutcome subsumes(@Nonnull final Coding codingA,
      @Nonnull final Coding codingB) {

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
}

