/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.TerminologyClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * @author John Grimes
 */
public abstract class ResourceDefinitions {

  private static final String BASE_RESOURCE_URL_PREFIX = "http://hl7.org/fhir/StructureDefinition/";

  private static final Map<String, StructureDefinition> resources = new HashMap<>();

  public static void ensureInitialized(TerminologyClient terminologyClient) {
    if (resources.isEmpty()) {
      List<StructureDefinition> structureDefinitions = terminologyClient
          .getAllStructureDefinitions();
      Function<StructureDefinition, StructureDefinition> fetchResourceWithId = entry -> terminologyClient
          .getStructureDefinitionById(new IdType(entry.getId()));
      resources.putAll(structureDefinitions.stream()
          .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId)));
    }
  }

  public static StructureDefinition getBaseResource(String resourceName) {
    return resources.get(BASE_RESOURCE_URL_PREFIX + resourceName);
  }

}
