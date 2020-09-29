package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhir.TerminologyClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to encapsulate the creation of a subsumes relation for a set of Codings.
 *
 * @author Piotr Szul
 */
class ClosureService {

  private static final Logger logger = LoggerFactory.getLogger(ClosureService.class);

  private final TerminologyClient terminologyClient;

  public ClosureService(TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  public Closure getSubsumesRelation(
      Collection<SimpleCoding> systemAndCodes) {
    List<Coding> codings =
        systemAndCodes.stream().map(SimpleCoding::toCoding).collect(Collectors.toList());
    // recreate the systemAndCodes dataset from the list not to execute the query again.
    // Create a unique name for the closure table for this code system, based upon the
    // expressions of the input, argument and the CodeSystem URI.
    String closureName = UUID.randomUUID().toString();
    logger.info("Sending $closure request to terminology service with name '{}' and {} codings",
        closureName, codings.size());
    // Execute the closure operation against the terminology server.
    // TODO: add validation checks for the response
    ConceptMap initialResponse = terminologyClient.closure(new StringType(closureName), null);
    validateConceptMap(initialResponse, closureName, "1");
    ConceptMap closureResponse =
        terminologyClient.closure(new StringType(closureName), codings);
    validateConceptMap(closureResponse, closureName, "2");
    return conceptMapToClosure(closureResponse);
  }

  private void validateConceptMap(ConceptMap conceptMap, String closureName, String version) {
    if (!PublicationStatus.ACTIVE.equals(conceptMap.getStatus())) {
      throw new RuntimeException(
          "Expected ConceptMap with status: ACTIVE, got: " + conceptMap.getStatus());
    }
    // TODO: Uncomment when testing is done
    // if (!closureName.equals(conceptMap.getName())) {
    // throw new RuntimeException("");
    // }
    // if (!version.equals(conceptMap.getVersion())) {
    // throw new RuntimeException("");
    // }
  }

  /**
   * According to the specification the only valid equivalences in the response are: equal,
   * specializes, subsumes and unmatched
   *
   * @return Mapping for subsumes relation i.e from -- subsumes --> to
   */
  private static Mapping appendSubsumesMapping(List<Mapping> mappings, SimpleCoding source,
      SimpleCoding target, ConceptMapEquivalence equivalence) {
    Mapping result = null;
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
        logger.warn("Ignoring unexpected equivalence: " + equivalence + " source: " + source
            + " target: " + target);
        break;
    }
    return result;
  }

  private static List<Mapping> conceptMapToMappings(ConceptMap conceptMap) {
    List<Mapping> mappings = new ArrayList<Mapping>();
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

  static Closure conceptMapToClosure(ConceptMap conceptMap) {
    return Closure.fromMappings(conceptMapToMappings(conceptMap));
  }
}
