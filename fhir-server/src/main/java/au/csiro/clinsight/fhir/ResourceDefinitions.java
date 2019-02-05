/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.TerminologyClient;
import ca.uhn.fhir.rest.api.SummaryEnum;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.ElementDefinition;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.dstu3.model.StructureDefinition.StructureDefinitionDifferentialComponent;
import org.hl7.fhir.dstu3.model.StructureDefinition.StructureDefinitionSnapshotComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central repository of FHIR resource definitions which can be looked up efficiently from across
 * the application.
 *
 * @author John Grimes
 */
public abstract class ResourceDefinitions {

  private static final Logger logger = LoggerFactory.getLogger(ResourceDefinitions.class);
  private static final String BASE_RESOURCE_URL_PREFIX = "http://hl7.org/fhir/StructureDefinition/";

  private static final Map<String, StructureDefinition> resources = new HashMap<>();

  /**
   * Fetches all StructureDefinitions known to the supplied terminology server, and loads them into
   * memory for later querying through the `getBaseResource` and `getElementDefinition` methods.
   */
  public static void ensureInitialized(TerminologyClient terminologyClient) {
    if (resources.isEmpty()) {
      List<StructureDefinition> structureDefinitions = terminologyClient
          .getAllStructureDefinitions(SummaryEnum.TRUE);
      Function<StructureDefinition, StructureDefinition> fetchResourceWithId = entry -> terminologyClient
          .getStructureDefinitionById(new IdType(entry.getId()));
      resources.putAll(structureDefinitions.stream()
          .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId)));
    }
  }

  /**
   * Returns the StructureDefinition for a base resource type. Returns null if the resource is not
   * found.
   */
  public static StructureDefinition getBaseResource(String resourceName) {
    return resources.get(BASE_RESOURCE_URL_PREFIX + resourceName);
  }

  /**
   * Returns the ElementDefinition matching a given path. Returns null if the path is not found.
   * Only works for base resources currently.
   */
  public static ElementDefinition getElementDefinition(String path) {
    // Get the StructureDefinition by extracting the first component of the path, then passing it to
    // the `getBaseResource` method.
    String[] pathComponents = path.split("\\.");
    String resourceName = pathComponents[0];
    StructureDefinition definition = getBaseResource(resourceName);
    if (definition == null) {
      return null;
    }

    // Search for the path within both the snapshot and differential elements of the
    // StructureDefinition.
    StructureDefinitionSnapshotComponent snapshot = definition.getSnapshot();
    StructureDefinitionDifferentialComponent differential = definition.getDifferential();
    if ((snapshot == null || snapshot.isEmpty()) &&
        (differential == null || differential.isEmpty())) {
      logger.warn("Encountered StructureDefinition with empty snapshot and differential: "
          + definition.getId());
      return null;
    }
    Predicate<ElementDefinition> matchesPath = element -> element.getPath().equals(path);
    if (snapshot != null) {
      Optional<ElementDefinition> element = snapshot.getElement().stream().filter(matchesPath)
          .findAny();
      if (element.isPresent()) {
        return element.get();
      }
    }
    if (differential != null) {
      return differential.getElement().stream().filter(matchesPath).findAny().orElse(null);
    }
    return null;
  }

}
