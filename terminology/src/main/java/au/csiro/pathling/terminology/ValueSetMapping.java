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

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ConceptReferenceComponent;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetComposeComponent;

/**
 * Input/output mappings for {@link ValueSet} related operations, such as {@code expand}.
 *
 * @author Piotr Szul
 */
@Slf4j
@Deprecated
public final class ValueSetMapping extends BaseMapping {

  private ValueSetMapping() {
  }

  @Value
  private static class CodeSystemReference {

    @Nonnull
    Optional<String> system;

    @Nonnull
    Optional<String> version;

    private boolean matchesCoding(@Nonnull final SimpleCoding coding) {
      if (system.isEmpty() || coding.getSystem() == null) {
        return false;
      }
      final boolean eitherSideIsMissingVersion =
          version.isEmpty() || coding.getVersion() == null;
      final boolean versionAgnosticTest = system.get().equals(coding.getSystem());
      if (eitherSideIsMissingVersion) {
        return versionAgnosticTest;
      } else {
        return versionAgnosticTest && version.get().equals(coding.getVersion());
      }
    }
  }

  /**
   * Constructs a {@link ValueSet} representing the intersection of a set of codings and a server
   *
   * @param valueSetUri the server defined value set.
   * @param codings the set of codings to intersect.
   * @return the intersection value set.
   */
  @Nonnull
  public static ValueSet toIntersection(@Nonnull final String valueSetUri,
      @Nonnull final Collection<SimpleCoding> codings) {
    final Set<CodeSystemReference> validCodeSystems = codings.stream()
        .filter(SimpleCoding::isDefined)
        .map(coding -> new CodeSystemReference(Optional.ofNullable(coding.getSystem()),
            Optional.ofNullable(coding.getVersion())))
        .collect(Collectors.toUnmodifiableSet());

    // Create a ValueSet to represent the intersection of the input codings and the ValueSet
    // described by the URI in the argument.
    final ValueSet intersection = new ValueSet();
    final ValueSetComposeComponent compose = new ValueSetComposeComponent();
    final List<ConceptSetComponent> includes = new ArrayList<>();

    // Create an include section for each unique code system present within the input codings.
    for (final CodeSystemReference codeSystem : validCodeSystems) {
      final ConceptSetComponent include = new ConceptSetComponent();
      include.setValueSet(Collections.singletonList(new CanonicalType(valueSetUri)));
      //noinspection OptionalGetWithoutIsPresent
      include.setSystem(codeSystem.getSystem().get());
      codeSystem.getVersion().ifPresent(include::setVersion);

      // Add the codings that match the current code system.
      final List<ConceptReferenceComponent> concepts = codings.stream()
          .filter(codeSystem::matchesCoding)
          .map(SimpleCoding::getCode)
          .filter(Objects::nonNull)
          .distinct()
          .map(code -> {
            final ConceptReferenceComponent concept = new ConceptReferenceComponent();
            concept.setCode(code);
            return concept;
          })
          .collect(Collectors.toList());

      if (!concepts.isEmpty()) {
        include.setConcept(concepts);
        includes.add(include);
      }
    }
    compose.setInclude(includes);
    intersection.setCompose(compose);
    return intersection;
  }


  /**
   * Build a set of {@link SimpleCoding} to represent the codings present in the value set.
   *
   * @param valueSet the value set
   * @return the set of simple codings the value set contains.
   */
  @Nonnull
  public static Set<SimpleCoding> codingSetFromExpansion(@Nullable final ValueSet valueSet) {
    if (valueSet == null) {
      return Collections.emptySet();
    }
    // Build a set of SimpleCodings to represent the codings present in the intersection.
    return valueSet.getExpansion().getContains().stream()
        .map(contains -> new SimpleCoding(contains.getSystem(), contains.getCode(),
            contains.getVersion()))
        .collect(Collectors.toSet());
  }

}
