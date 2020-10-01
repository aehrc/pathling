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

  public ClosureService(@Nonnull TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  @Nonnull
  public Closure getSubsumesRelation(
      @Nonnull final Collection<SimpleCoding> systemAndCodes) {
    List<Coding> codings =
        systemAndCodes.stream().map(SimpleCoding::toCoding).collect(Collectors.toList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    String closureName = UUID.randomUUID().toString();
    log.info("Sending $closure request to terminology service with name '{}' and {} codings",
        closureName, codings.size());
    // Execute the closure operation against the terminology server.
    // TODO: add validation checks for the response
    ConceptMap initialResponse = terminologyClient.closure(new StringType(closureName), null);
    ConceptMap closureResponse =
        terminologyClient.closure(new StringType(closureName), codings);
    return conceptMapToClosure(closureResponse);
  }


  /**
   * According to the specification the only valid equivalences in the response are: equal,
   * specializes, subsumes and unmatched
   *
   * @return Mapping for subsumes relation i.e from -- subsumes --> to
   */
  private static void appendSubsumesMapping(@Nonnull final List<Mapping> mappings,
      @Nonnull final SimpleCoding source,
      @Nonnull final SimpleCoding target, @Nonnull final ConceptMapEquivalence equivalence) {
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
      List<ConceptMapGroupComponent> groups = conceptMap.getGroup();
      for (ConceptMapGroupComponent group : groups) {
        List<SourceElementComponent> elements = group.getElement();
        for (SourceElementComponent source : elements) {
          for (TargetElementComponent target : source.getTarget()) {
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
  static Closure conceptMapToClosure(@Nonnull final ConceptMap conceptMap) {
    return Closure.fromMappings(conceptMapToMappings(conceptMap));
  }
}
