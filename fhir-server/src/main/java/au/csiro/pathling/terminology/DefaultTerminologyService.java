/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static au.csiro.pathling.terminology.ClosureMapping.relationFromConceptMap;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.ValueSet.ConceptReferenceComponent;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetComposeComponent;

/**
 * Default implementation of TerminologyService using a backend terminology server.
 */
@Slf4j
public class DefaultTerminologyService implements TerminologyService {


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


  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final TerminologyClient terminologyClient;

  /**
   * @param fhirContext The {@link FhirContext} used to interpret responses
   * @param terminologyClient The {@link TerminologyClient} used to issue requests
   */
  public DefaultTerminologyService(@Nonnull final FhirContext fhirContext,
      @Nonnull final TerminologyClient terminologyClient) {
    this.fhirContext = fhirContext;
    this.terminologyClient = terminologyClient;
  }

  @Nonnull
  private Stream<SimpleCoding> filterByKnownSystems(
      @Nonnull final Collection<SimpleCoding> codings) {

    // filter out codings with code systems unknown to the terminology server
    final Set<String> allCodeSystems = codings.stream()
        .filter(SimpleCoding::isDefined)
        .map(SimpleCoding::getSystem)
        .collect(Collectors.toSet());

    final Set<String> knownCodeSystems = allCodeSystems.stream().filter(codeSystem -> {
      final UriParam uri = new UriParam(codeSystem);
      final List<CodeSystem> knownSystems = terminologyClient.searchCodeSystems(
          uri, new HashSet<>(Collections.singletonList("id")));
      return !(knownSystems == null || knownSystems.isEmpty());
    }).collect(Collectors.toSet());

    if (!knownCodeSystems.equals(allCodeSystems)) {
      final Collection<String> unrecognizedCodeSystems = new HashSet<>(allCodeSystems);
      unrecognizedCodeSystems.removeAll(knownCodeSystems);
      log.warn("Terminology server does not recognize these coding systems: {}",
          unrecognizedCodeSystems);
    }

    return codings.stream()
        .filter(coding -> knownCodeSystems.contains(coding.getSystem()));
  }

  @Nonnull
  @Override
  public ConceptTranslator translate(@Nonnull final Collection<SimpleCoding> codings,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final Collection<ConceptMapEquivalence> equivalences) {

    final List<SimpleCoding> uniqueCodings = codings.stream().distinct()
        .collect(Collectors.toUnmodifiableList());
    // create bundle
    final Bundle translateBatch = TranslateMapping
        .toRequestBundle(uniqueCodings, conceptMapUrl, reverse);
    final Bundle result = terminologyClient.batch(translateBatch);
    return TranslateMapping
        .fromResponseBundle(checkNotNull(result), uniqueCodings, equivalences, fhirContext);
  }

  @Nonnull
  @Override
  public Relation getSubsumesRelation(@Nonnull Collection<SimpleCoding> systemAndCodes) {
    final List<Coding> codings = filterByKnownSystems(systemAndCodes)
        .distinct()
        .map(SimpleCoding::toCoding)
        .collect(Collectors.toUnmodifiableList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    final String closureName = UUID.randomUUID().toString();
    log.info("Sending $closure request to terminology service with name '{}' and {} codings",
        closureName, codings.size());
    terminologyClient.initialiseClosure(new StringType(closureName));
    final ConceptMap closureResponse =
        terminologyClient.closure(new StringType(closureName), codings);
    checkNotNull(closureResponse);
    return relationFromConceptMap(closureResponse);
  }

  @Nonnull
  @Override
  public Set<SimpleCoding> intersect(@Nonnull String valueSetUri,
      @Nonnull Collection<SimpleCoding> systemAndCodes) {
    final Set<SimpleCoding> codings = systemAndCodes.stream()
        .filter(Objects::nonNull)
        .filter(SimpleCoding::isDefined)
        .collect(Collectors.toSet());

    final Set<CodeSystemReference> codeSystems = codings.stream()
        .map(coding -> new CodeSystemReference(Optional.ofNullable(coding.getSystem()),
            Optional.ofNullable(coding.getVersion())))
        .filter(codeSystem -> codeSystem.getSystem().isPresent())
        .collect(Collectors.toSet());

    // Filter the set of code systems to only those known by the terminology server. We determine
    // this by performing a CodeSystem search operation.
    final Collection<String> uniqueKnownUris = new HashSet<>();
    for (final CodeSystemReference codeSystem : codeSystems) {
      //noinspection OptionalGetWithoutIsPresent
      final UriParam uri = new UriParam(codeSystem.getSystem().get());
      final List<CodeSystem> knownSystems = terminologyClient.searchCodeSystems(
          uri, new HashSet<>(Collections.singletonList("id")));
      if (knownSystems != null && knownSystems.size() > 0) {
        uniqueKnownUris.add(codeSystem.getSystem().get());
      }
    }
    //noinspection OptionalGetWithoutIsPresent
    final Set<CodeSystemReference> filteredCodeSystems = codeSystems.stream()
        .filter(codeSystem -> uniqueKnownUris.contains(codeSystem.getSystem().get()))
        .collect(Collectors.toSet());

    // Create a ValueSet to represent the intersection of the input codings and the ValueSet
    // described by the URI in the argument.
    final ValueSet intersection = new ValueSet();
    final ValueSetComposeComponent compose = new ValueSetComposeComponent();
    final List<ConceptSetComponent> includes = new ArrayList<>();

    // Create an include section for each unique code system present within the input codings.
    for (final CodeSystemReference codeSystem : filteredCodeSystems) {
      final ConceptSetComponent include = new ConceptSetComponent();
      include.setValueSet(Collections.singletonList(new CanonicalType(valueSetUri)));
      //noinspection OptionalGetWithoutIsPresent
      include.setSystem(codeSystem.getSystem().get());
      codeSystem.getVersion().ifPresent(include::setVersion);

      // Add the codings that match the current code system.
      final List<ConceptReferenceComponent> concepts = codings.stream()
          .filter(codeSystem::matchesCoding)
          .map(coding -> {
            final ConceptReferenceComponent concept = new ConceptReferenceComponent();
            concept.setCode(coding.getCode());
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

    final Set<SimpleCoding> expandedCodings;
    if (includes.isEmpty()) {
      // If there is nothing to expand, don't bother calling the terminology server.
      expandedCodings = Collections.emptySet();
    } else {
      // Ask the terminology service to work out the intersection between the set of input codings
      // and the ValueSet identified by the URI in the argument.
      log.info("Intersecting {} concepts with {} using terminology service", codings.size(),
          valueSetUri);
      final ValueSet expansion = terminologyClient
          .expand(intersection, new IntegerType(codings.size()));
      if (expansion == null) {
        return Collections.emptySet();
      }

      // Build a set of SimpleCodings to represent the codings present in the intersection.
      expandedCodings = expansion.getExpansion().getContains().stream()
          .map(contains -> new SimpleCoding(contains.getSystem(), contains.getCode(),
              contains.getVersion()))
          .collect(Collectors.toSet());
    }
    return expandedCodings;
  }
}
