/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * Input/output mappings for $closure operation.
 *
 * @author Piotr Szul
 * @see <a href="https://www.hl7.org/fhir/terminology-service.html#closure">Maintaining a Closure
 * Table</a>
 * @see <a href="https://www.hl7.org/fhir/conceptmap-operation-closure.html">Operation $closure on
 * ConceptMap</a>
 */
@Slf4j
public final class ClosureMapping extends BaseMapping {

  private ClosureMapping() {
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
  public static Relation closureFromConceptMap(@Nonnull final ConceptMap conceptMap) {
    return Relation.fromMappings(conceptMapToMappings(conceptMap));
  }
}
