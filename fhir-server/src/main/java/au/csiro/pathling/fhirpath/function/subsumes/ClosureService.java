/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.StringType;

/**
 * Helper class to encapsulate the creation of a subsumes relation for a set of Codings.
 * <p>
 * Additional resources on closure table maintenance:
 *
 * @author Piotr Szul
 * @see <a href="https://www.hl7.org/fhir/terminology-service.html#closure">Maintaining a Closure
 * Table</a>
 * @see <a href="https://www.hl7.org/fhir/conceptmap-operation-closure.html">Operation $closure on
 * ConceptMap</a>
 */
@Slf4j
class ClosureService {

  @Nonnull
  private final TerminologyClient terminologyClient;

  public ClosureService(@Nonnull final TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  /**
   * According to the specification the only valid equivalences in the response are: equal,
   * specializes, subsumes and unmatched.
   */
  private static void appendSubsumesMapping(@Nonnull final Collection<Mapping> mappings,
      @Nonnull final SimpleCoding source, @Nonnull final SimpleCoding target,
      @Nonnull final ConceptMapEquivalence equivalence) {
    switch (equivalence) {
      case SUBSUMES:
        mappings.add(Mapping.of(target, source));
        break;
      case SPECIALIZES:
        mappings.add(Mapping.of(source, target));
        break;
      case EQUAL:
        mappings.add(Mapping.of(source, target));
        mappings.add(Mapping.of(target, source));
        break;
      case UNMATCHED:
        break;
      default:
        log.warn("Ignoring unexpected equivalence: " + equivalence + " source: " + source
            + " target: " + target);
        break;
    }
  }

  @Nonnull
  private static List<Mapping> conceptMapToMappings(@Nonnull final ConceptMap conceptMap) {
    final List<Mapping> mappings = new ArrayList<>();
    if (conceptMap.hasGroup()) {
      final List<ConceptMapGroupComponent> groups = conceptMap.getGroup();
      for (final ConceptMapGroupComponent group : groups) {
        final List<SourceElementComponent> elements = group.getElement();
        for (final SourceElementComponent source : elements) {
          for (final TargetElementComponent target : source.getTarget()) {
            appendSubsumesMapping(mappings,
                new SimpleCoding(group.getSource(), source.getCode(), group.getSourceVersion()),
                new SimpleCoding(group.getTarget(), target.getCode(), group.getTargetVersion()),
                target.getEquivalence());
          }
        }
      }
    }
    return mappings;
  }

  @Nonnull
  public Closure getSubsumesRelation(
      @Nonnull final Collection<SimpleCoding> systemAndCodes) {
    final List<Coding> codings =
        systemAndCodes.stream().map(SimpleCoding::toCoding).collect(Collectors.toList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    final String closureName = UUID.randomUUID().toString();
    log.info("Sending $closure request to terminology service with name '{}' and {} codings",
        closureName, codings.size());
    terminologyClient.closure(new StringType(closureName), null);
    final ConceptMap closureResponse =
        terminologyClient.closure(new StringType(closureName), codings);
    return conceptMapToClosure(closureResponse);
  }

  @Nonnull
  static Closure conceptMapToClosure(@Nonnull final ConceptMap conceptMap) {
    return Closure.fromMappings(conceptMapToMappings(conceptMap));
  }
}
