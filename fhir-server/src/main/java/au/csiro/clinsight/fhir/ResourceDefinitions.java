/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.utilities.Strings;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.ElementDefinition;
import org.hl7.fhir.dstu3.model.ElementDefinition.TypeRefComponent;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.dstu3.model.StructureDefinition.StructureDefinitionDifferentialComponent;
import org.hl7.fhir.dstu3.model.StructureDefinition.StructureDefinitionKind;
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
      // Do a search to get all the StructureDefinitions. Unfortunately the `kind` search parameter
      // is not supported by Ontoserver, yet.
      List<StructureDefinition> structureDefinitions = terminologyClient
          .getAllStructureDefinitions(Sets.newHashSet("url", "kind"));

      // Fetch each StructureDefinition and create a HashMap keyed on URL. Filter out anything that
      // is not a resource definition.
      Function<StructureDefinition, StructureDefinition> fetchResourceWithId = entry -> terminologyClient
          .getStructureDefinitionById(new IdType(entry.getId()));
      resources.putAll(structureDefinitions.stream()
          .filter(sd -> sd.getKind() == StructureDefinitionKind.RESOURCE)
          .peek(sd -> logger
              .info("Retrieving StructureDefinition: " + sd.getUrl()))
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
  public static ResolvedElement getElementDefinition(String path) {
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
    Map<String, ResolvedElement> resolvedElements = new HashMap<>();
    if (snapshot != null) {
      updateResolvedElementsMap(resolvedElements, snapshot.getElement());
    }
    if (differential != null) {
      updateResolvedElementsMap(resolvedElements, differential.getElement());
    }
    ResolvedElement result = resolvedElements.get(path);
    if (result != null) {
      return result;
    }
    return null;
  }

  private static void updateResolvedElementsMap(Map<String, ResolvedElement> resolvedElements,
      List<ElementDefinition> elements) {
    for (ElementDefinition element : elements) {
      List<TypeRefComponent> typeRefComponents = element.getType();
      if (typeRefComponents == null || typeRefComponents.isEmpty()) {
        continue;
      }
      String elementPath = element.getPath();
      checkNotNull(elementPath, "Encountered element with no path");
      if (elementPath.matches(".*\\[x]$")) {
        List<String> types = typeRefComponents.stream().map(TypeRefComponent::getCode)
            .collect(Collectors.toList());
        for (String type : types) {
          checkNotNull(type, "Encountered element type with no code");
          String transformedPath = elementPath
              .replaceAll("\\[x]", Strings.capitalize(type));
          resolvedElements.put(transformedPath, new ResolvedElement(transformedPath, type));
        }
      } else {
        String typeCode = typeRefComponents.get(0).getCode();
        checkNotNull(typeCode, "Encountered element type with no code");
        resolvedElements.put(elementPath, new ResolvedElement(elementPath, typeCode));
      }
    }
  }

  public static class ResolvedElement {

    private String path;
    private String typeCode;

    public ResolvedElement(String path, String typeCode) {
      this.path = path;
      this.typeCode = typeCode;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public String getTypeCode() {
      return typeCode;
    }

    public void setTypeCode(String typeCode) {
      this.typeCode = typeCode;
    }

  }

}
