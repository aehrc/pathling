/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.utilities.Strings;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
  private static final Map<String, StructureDefinition> complexTypes = new HashMap<>();

  private static final Set<String> supportedPrimitiveTypes = Sets.newHashSet(
      "decimal",
      "markdown",
      "id",
      "dateTime",
      "time",
      "date",
      "code",
      "string",
      "uri",
      "oid",
      "integer",
      "unsignedInt",
      "positiveInt",
      "boolean",
      "instant"
  );
  private static final Set<String> supportedComplexTypes = Sets.newHashSet(
      "Ratio",
      "Period",
      "Range",
      "Attachment",
      "Identifier",
      "HumanName",
      "Annotation",
      "Address",
      "ContactPoint",
      "SampledData",
      "Money",
      "Count",
      "Duration",
      "SimpleQuantity",
      "Quantity",
      "Distance",
      "Age",
      "CodeableConcept",
      "Signature",
      "Coding",
      "Timing"
  );

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

      Function<StructureDefinition, StructureDefinition> fetchResourceWithId = entry -> terminologyClient
          .getStructureDefinitionById(new IdType(entry.getId()));

      // Fetch each resource StructureDefinition and create a HashMap keyed on URL.
      resources.putAll(structureDefinitions.stream()
          .filter(sd -> sd.getKind() == StructureDefinitionKind.RESOURCE)
          .peek(sd -> logger
              .info("Retrieving resource StructureDefinition: " + sd.getUrl()))
          .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId)));

      Supplier<Predicate<StructureDefinition>> complexTypeFilter = () -> sd -> {
        // Check that the StructureDefinition is a complex type, and that the URL matches the base
        // FHIR prefix.
        if (sd.getKind() != StructureDefinitionKind.COMPLEXTYPE ||
            !sd.getUrl().matches("^http://hl7.org/fhir/StructureDefinition/.+")) {
          return false;
        }
        // Check that the last component of the URL matches one of our supported complex types.
        String[] components = sd.getUrl().split("/");
        if (components.length == 0) {
          return false;
        } else {
          return supportedComplexTypes.contains(components[components.length - 1]);
        }
      };

      // Fetch each complex type StructureDefinition (just the ones that are part of the base spec)
      // and create a HashMap keyed on URL.
      complexTypes.putAll(structureDefinitions.stream()
          .filter(complexTypeFilter.get())
          .peek(sd -> logger
              .info("Retrieving complex type StructureDefinition: " + sd.getUrl()))
          .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId)));
      if (complexTypes.size() != supportedComplexTypes.size()) {
        Set<String> difference = supportedComplexTypes.stream()
            .map(t -> BASE_RESOURCE_URL_PREFIX + t).collect(Collectors.toSet());
        difference.removeAll(complexTypes.keySet());
        logger.warn("Number of complex type definitions retrieved does not equal number of "
            + "supported complex types, missing: " + String.join(", ", difference));
      }
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
   * Returns the StructureDefinition for a base complex type. Returns null if the resource is not
   * found.
   */
  public static StructureDefinition getComplexType(String typeName) {
    return complexTypes.get(BASE_RESOURCE_URL_PREFIX + typeName);
  }

  /**
   * Returns the ElementDefinition matching a given path. Returns null if the path is not found.
   * Only works for base resources currently.
   */
  public static ResolvedElement getElementDefinition(String path) {
    return getElementDefinition(path, null);
  }

  private static ResolvedElement getElementDefinition(String path, String complexType)
      throws ResourceNotKnownException, ElementNotKnownException {
    LinkedList<String> head = new LinkedList<>(Arrays.asList(path.split("\\.")));
    LinkedList<String> tail = new LinkedList<>();

    // Get the StructureDefinition by extracting the first component of the path, then passing it to
    // the `getBaseResource` method.
    String resourceName = head.get(0);
    StructureDefinition definition;
    if (complexType == null) {
      definition = getBaseResource(resourceName);
      if (definition == null) {
        throw new ResourceNotKnownException("Resource not known: " + resourceName);
      }
    } else {
      definition = getComplexType(complexType);
      if (definition == null) {
        throw new AssertionError("Encountered unknown complex type: " + complexType);
      }
    }

    // Scan the full path, then remove the last component and scan again, etc.
    while (head.size() > 1) {
      StructureDefinitionSnapshotComponent snapshot = definition.getSnapshot();
      StructureDefinitionDifferentialComponent differential = definition.getDifferential();

      // Check that the StructureDefinition has at least one of snapshot and differential.
      //
      // TODO: Figure out what to do about elements that are inherited.
      if ((snapshot == null || snapshot.isEmpty()) &&
          (differential == null || differential.isEmpty())) {
        throw new AssertionError(
            "Encountered StructureDefinition with empty snapshot and differential: "
                + definition.getId());
      }
      // Build a map of elements and their data types.
      //
      // TODO: Move this to `ensureInitialized`.
      Map<String, ResolvedElement> resolvedElements = new HashMap<>();
      if (snapshot != null) {
        updateResolvedElementsMap(resolvedElements, snapshot.getElement());
      }
      if (differential != null) {
        updateResolvedElementsMap(resolvedElements, differential.getElement());
      }

      ResolvedElement result = resolvedElements.get(String.join(".", head));
      if (result == null) {
        // Chop a component off the end and scan again. Keep the tail for constructing the path for
        // complex types.
        tail.push(head.get(head.size() - 1));
        head.remove(head.size() - 1);
      } else if (supportedPrimitiveTypes.contains(result.getTypeCode())) {
        // If the element is a primitive, stop here and return the result.
        return result;
      } else if (supportedComplexTypes.contains(result.getTypeCode())) {
        if (tail.isEmpty()) {
          // If the tail is empty, it means that a complex type is the last component of the path.
          return result;
        } else {
          // If the element is complex and a path within it has been requested, recurse into
          // scanning the definition of that type.
          String newPath = String.join(".", result.getTypeCode(), String.join(".", tail));
          return getElementDefinition(newPath, result.getTypeCode());
        }
      } else {
        throw new AssertionError("Encountered unknown type: " + result.getTypeCode());
      }
    }
    throw new ElementNotKnownException("Element path not known: " + path);
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

  public static boolean isSupportedPrimitive(String fhirType) {
    return supportedPrimitiveTypes.contains(fhirType);
  }

  public static boolean isSupportedComplex(String fhirType) {
    return supportedComplexTypes.contains(fhirType);
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

  public static class ResourceNotKnownException extends RuntimeException {

    public ResourceNotKnownException(String message) {
      super(message);
    }

  }

  public static class ElementNotKnownException extends RuntimeException {

    public ElementNotKnownException(String message) {
      super(message);
    }

  }
}
