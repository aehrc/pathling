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

import static au.csiro.pathling.terminology.ClosureMapping.relationFromConceptMap;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * Default implementation of TerminologyService using a backend terminology server.
 */
@Slf4j
@Deprecated
public class DefaultTerminologyService implements TerminologyService {

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final TerminologyClient terminologyClient;


  @Nonnull
  private final UUIDFactory uuidFactory;

  /**
   * @param fhirContext The {@link FhirContext} used to interpret responses.
   * @param terminologyClient The {@link TerminologyClient} used to issue requests.
   * @param uuidFactory The {@link UUIDFactory} used to create UUIDs.
   */
  public DefaultTerminologyService(@Nonnull final FhirContext fhirContext,
      @Nonnull final TerminologyClient terminologyClient,
      @Nonnull final UUIDFactory uuidFactory) {
    this.fhirContext = fhirContext;
    this.terminologyClient = terminologyClient;
    this.uuidFactory = uuidFactory;
  }


  private boolean isValidCoding(@Nullable final SimpleCoding coding) {
    return Objects.nonNull(coding) && coding.isDefined();
  }

  private boolean isKnownSystem(@Nonnull final String codeSystem) {
    final UriParam uri = new UriParam(codeSystem);
    final List<CodeSystem> knownSystems = terminologyClient.searchCodeSystems(
        uri, new HashSet<>(Collections.singletonList("id")));
    return !(knownSystems == null || knownSystems.isEmpty());
  }

  @Nonnull
  private Stream<SimpleCoding> validCodings(
      @Nonnull final Collection<SimpleCoding> codings) {
    return codings.stream()
        .filter(this::isValidCoding);
  }

  @Nonnull
  private Stream<SimpleCoding> validAndKnownCodings(
      @Nonnull final Collection<SimpleCoding> codings) {

    // filter out codings with code systems unknown to the terminology server
    final Set<String> allCodeSystems = validCodings(codings)
        .map(SimpleCoding::getSystem)
        .collect(Collectors.toSet());

    final Set<String> knownCodeSystems = allCodeSystems.stream()
        .filter(this::isKnownSystem)
        .collect(Collectors.toSet());

    if (!knownCodeSystems.equals(allCodeSystems)) {
      final Collection<String> unrecognizedCodeSystems = new HashSet<>(allCodeSystems);
      unrecognizedCodeSystems.removeAll(knownCodeSystems);
      log.warn("Terminology server does not recognize these coding systems: {}",
          unrecognizedCodeSystems);
    }
    return validCodings(codings)
        .filter(coding -> knownCodeSystems.contains(coding.getSystem()));
  }
  
  @Nonnull
  @Override
  public ConceptTranslator translate(@Nonnull final Collection<SimpleCoding> codings,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final Collection<ConceptMapEquivalence> equivalences,
      @Nullable final String target) {

    final List<SimpleCoding> uniqueCodings = validCodings(codings)
        .distinct()
        .collect(Collectors.toUnmodifiableList());

    final Set<ConceptMapEquivalence> uniqueEquivalences = equivalences.stream()
        .collect(Collectors.toUnmodifiableSet());

    // create bundle
    if (!uniqueCodings.isEmpty() && !uniqueEquivalences.isEmpty()) {
      final Bundle translateBatch = TranslateMapping
          .toRequestBundle(uniqueCodings, conceptMapUrl, reverse, target);
      final Bundle result = terminologyClient.batch(translateBatch);
      return TranslateMapping
          .fromResponseBundle(checkNotNull(result), uniqueCodings, uniqueEquivalences,
              fhirContext);
    } else {
      return ConceptTranslator.empty();
    }
  }

  @Nonnull
  @Override
  public Relation getSubsumesRelation(@Nonnull final Collection<SimpleCoding> systemAndCodes) {
    final List<Coding> codings = validAndKnownCodings(systemAndCodes)
        .distinct()
        .map(SimpleCoding::toCoding)
        .collect(Collectors.toUnmodifiableList());
    if (!codings.isEmpty()) {
      // recreate the systemAndCodes dataset from the list not to execute the query again.
      // Create a unique name for the closure table for this code system, based upon the
      // expressions of the input, argument and the CodeSystem URI.
      final String closureName = uuidFactory.nextUUID().toString();
      log.info("Sending $closure request to terminology service with name '{}' and {} codings",
          closureName, codings.size());
      terminologyClient.initialiseClosure(new StringType(closureName));
      final ConceptMap closureResponse =
          terminologyClient.closure(new StringType(closureName), codings);
      checkNotNull(closureResponse);
      return relationFromConceptMap(closureResponse);
    } else {
      return Relation.equality();
    }
  }

  @Nonnull
  @Override
  public Set<SimpleCoding> intersect(@Nonnull final String valueSetUri,
      @Nonnull final Collection<SimpleCoding> systemAndCodes) {
    final Set<SimpleCoding> codings = validAndKnownCodings(systemAndCodes)
        .collect(Collectors.toSet());

    // Create a ValueSet to represent the intersection of the input codings and the ValueSet
    // described by the URI in the argument.
    final ValueSet intersection = ValueSetMapping.toIntersection(valueSetUri, codings);

    final Set<SimpleCoding> expandedCodings;
    if (intersection.getCompose().getInclude().isEmpty()) {
      // If there is nothing to expand, don't bother calling the terminology server.
      expandedCodings = Collections.emptySet();
    } else {
      // Ask the terminology service to work out the intersection between the set of input codings
      // and the ValueSet identified by the URI in the argument.
      log.info("Intersecting {} concepts with {} using terminology service", codings.size(),
          valueSetUri);
      final ValueSet expansion = terminologyClient
          .expand(intersection, new IntegerType(codings.size()));
      expandedCodings = ValueSetMapping.codingSetFromExpansion(expansion);
    }
    return expandedCodings;
  }
}
