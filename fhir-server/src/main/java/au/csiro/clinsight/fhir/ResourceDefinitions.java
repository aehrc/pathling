/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.utilities.Strings;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.ElementDefinition;
import org.hl7.fhir.dstu3.model.ElementDefinition.TypeRefComponent;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.dstu3.model.StructureDefinition.StructureDefinitionKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Central repository of FHIR resource complexTypes which can be looked up efficiently from across
 * the application.
 *
 * @author John Grimes
 */
public abstract class ResourceDefinitions {

  private static final Logger logger = LoggerFactory.getLogger(ResourceDefinitions.class);
  private static final String BASE_RESOURCE_URL_PREFIX = "http://hl7.org/fhir/StructureDefinition/";
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
  private static Map<String, Map<String, SummarisedElement>> resourceElements = new HashMap<>();
  private static Map<String, Map<String, SummarisedElement>> complexTypeElements = new HashMap<>();
  private static Map<String, StructureDefinition> resources = new HashMap<>();
  private static Map<String, StructureDefinition> complexTypes = new HashMap<>();

  /**
   * Fetches all StructureDefinitions known to the supplied terminology server, and loads them into
   * memory for later querying through the `getBaseResource` and `resolveElement` methods.
   */
  public static void ensureInitialized(TerminologyClient terminologyClient) {
    if (resources.isEmpty()) {
      // Do a search to get all the StructureDefinitions. Unfortunately the `kind` search parameter
      // is not supported by Ontoserver, yet.
      List<StructureDefinition> structureDefinitions = terminologyClient
          .getAllStructureDefinitions(Sets.newHashSet("url", "kind"));

      // Create a function that knows how to retrieve a StructureDefinition from the terminology
      // server.
      Function<StructureDefinition, StructureDefinition> fetchResourceWithId = entry -> terminologyClient
          .getStructureDefinitionById(new IdType(entry.getId()));

      // Fetch each resource StructureDefinition and create a HashMap keyed on URL.
      resources = retrieveResourceDefinitions(structureDefinitions, fetchResourceWithId);

      // Fetch each complex type StructureDefinition (just the ones that are part of the base spec)
      // and create a HashMap keyed on URL.
      complexTypes = retrieveComplexTypeDefinitions(structureDefinitions, fetchResourceWithId);

      // Check that all definitions have a snapshot element.
      validateDefinitions(resources.values());
      validateDefinitions(complexTypes.values());

      // Build a map of element paths and key information from their ElementDefinitions, for each
      // resource and complex type.
      resourceElements = summariseDefinitions(resources.values());
      complexTypeElements = summariseDefinitions(complexTypes.values());
    }
  }

  private static Map<String, StructureDefinition> retrieveResourceDefinitions(
      List<StructureDefinition> structureDefinitions,
      Function<StructureDefinition, StructureDefinition> fetchResourceWithId) {
    return structureDefinitions.stream()
        .filter(sd -> sd.getKind() == StructureDefinitionKind.RESOURCE)
        .peek(sd -> logger
            .info("Retrieving resource StructureDefinition: " + sd.getUrl()))
        .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId));
  }

  private static Map<String, StructureDefinition> retrieveComplexTypeDefinitions(
      List<StructureDefinition> structureDefinitions,
      Function<StructureDefinition, StructureDefinition> fetchResourceWithId) {
    // Create a filter that matches only base complex types.
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

    // Retrieve the StructureDefinitions that match the complex type filter.
    Map<String, StructureDefinition> definitions = structureDefinitions.stream()
        .filter(complexTypeFilter.get())
        .peek(sd -> logger
            .info("Retrieving complex type StructureDefinition: " + sd.getUrl()))
        .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId));

    // Do a sanity check that we were able to retrieve all of the supported types.
    if (definitions.size() != supportedComplexTypes.size()) {
      Set<String> difference = supportedComplexTypes.stream()
          .map(t -> BASE_RESOURCE_URL_PREFIX + t).collect(Collectors.toSet());
      difference.removeAll(definitions.keySet());
      logger.warn("Number of complex type complexTypes retrieved does not equal number of "
          + "supported complex types, missing: " + String.join(", ", difference));
    }

    return definitions;
  }

  private static void validateDefinitions(Collection<StructureDefinition> definitions) {
    definitions.forEach(definition -> {
      if (definition.getSnapshot() == null || definition.getSnapshot().isEmpty()) {
        throw new AssertionError(
            "Encountered StructureDefinition with empty snapshot: " + definition.getUrl());
      }
    });
  }

  private static Map<String, Map<String, SummarisedElement>> summariseDefinitions(
      Collection<StructureDefinition> definitions) {
    Map<String, Map<String, SummarisedElement>> result = new HashMap<>();
    for (StructureDefinition definition : definitions) {
      Map<String, SummarisedElement> summarisedElements = summariseElements(
          definition.getSnapshot().getElement());
      result.put(definition.getUrl(), summarisedElements);
    }
    return result;
  }

  /**
   * Returns the StructureDefinition for a base resource type. Returns null if the resource is not
   * found.
   */
  public static StructureDefinition getBaseResource(String resourceName) {
    checkInitialised();
    return resources.get(BASE_RESOURCE_URL_PREFIX + resourceName);
  }

  private static StructureDefinition getType(String typeName) {
    StructureDefinition result = resources.get(BASE_RESOURCE_URL_PREFIX + typeName);
    return result == null
        ? complexTypes.get(BASE_RESOURCE_URL_PREFIX + typeName)
        : result;
  }

  private static Map<String, SummarisedElement> getElementsForType(String typeName) {
    Map<String, SummarisedElement> result = resourceElements
        .get(BASE_RESOURCE_URL_PREFIX + typeName);
    return result == null
        ? complexTypeElements.get(BASE_RESOURCE_URL_PREFIX + typeName)
        : result;
  }

  /**
   * Returns the ElementDefinition matching a given path. Returns null if the path is not found.
   * Only works for base resources currently.
   */
  public static ResolvedElement resolveElement(@Nonnull String path)
      throws ResourceNotKnownException, ElementNotKnownException {
    checkInitialised();

    LinkedList<String> tail = new LinkedList<>(Arrays.asList(path.split("\\.")));
    LinkedList<String> head = new LinkedList<>();
    head.push(tail.pop());
    String resourceOrDataType = head.peek();

    // Get the StructureDefinition that describes the parent Resource or DataType.
    Map<String, SummarisedElement> elements = getElementsForType(resourceOrDataType);

    // Initialise a new object to hold the result.
    ResolvedElement result = new ResolvedElement(path);

    // Go through the full path, starting with just the first component and adding one component
    // with each iteration of the loop. This allows us to pick up multiple cardinalities and
    // transitions into complex types.
    while (true) {
      String currentPath = String.join(".", head);
      SummarisedElement element = elements.get(currentPath);

      // If the path cannot be found in the set of valid elements for this definition, loop back
      // around, add another component and try again. Also skip elements without a type or
      // cardinality, i.e. root elements.
      if (element == null || element.getTypeCode() == null || element.getMaxCardinality() == null) {
        head.add(tail.pop());
        continue;
      }

      // Save information about a multi-value traversal, if this is one.
      harvestMultiValueTraversal(result, currentPath, element);

      // Examine the type of the element.
      String typeCode = element.getTypeCode();

      // If this is a `BackboneElement`, we neither want to return yet or recurse into a complex
      // type definition.
      if (typeCode.equals("BackboneElement")) {
        head.add(tail.pop());
        continue;
      }
      return examineLeafElement(tail, result, currentPath, typeCode);
    }
  }

  private static void harvestMultiValueTraversal(@Nonnull ResolvedElement result,
      @Nonnull String currentPath,
      @Nonnull SummarisedElement element) {
    // If this element has a max cardinality of greater than one, record some information about
    // this traversal. This will be packaged up with the final result, and used to do things
    // like constructing joins when it comes time to build a query.
    assert element.getMaxCardinality() != null;
    if (!element.getMaxCardinality().equals("1")) {
      // If the current element is complex, we need to look up the children for it from the
      // element map for that type.
      if (isSupportedComplex(element.getTypeCode())) {
        SummarisedElement complexElement = getElementsForType(element.typeCode)
            .get(element.getTypeCode());
        result.getMultiValueTraversals().put(currentPath, complexElement.getChildElements());
      } else {
        result.getMultiValueTraversals().put(currentPath, element.getChildElements());
      }
    }
  }

  private static ResolvedElement examineLeafElement(LinkedList<String> tail, ResolvedElement result,
      String currentPath, String typeCode) {
    if (supportedPrimitiveTypes.contains(typeCode)) {
      result.setTypeCode(typeCode);

      // If the element is a primitive, stop here and return the result.
      return result;
    } else if (supportedComplexTypes.contains(typeCode)) {
      if (tail.isEmpty()) {
        // If the tail is empty, it means that a complex type is the last component of the path.
        return result;
      } else {
        // If the element is complex and a path within it has been requested, recurse into
        // scanning the definition of that type.
        String newPath = String.join(".", typeCode, String.join(".", tail));
        ResolvedElement nestedResult = resolveElement(newPath);

        // Translate the keys within the multi-value traversals of the nested results. This will,
        // for example, translate `CodeableConcept.coding` into
        // `Patient.communication.language.coding` for inclusion in the final result.
        Map<String, Set<String>> translatedTraversals = nestedResult.getMultiValueTraversals();
        for (String key : translatedTraversals.keySet()) {
          translatedTraversals.put(key.replace(typeCode, currentPath),
              translatedTraversals.get(key));
          translatedTraversals.remove(key);
        }

        // Merge the nested result into the final result and return.
        result.getMultiValueTraversals().putAll(translatedTraversals);
        result.setTypeCode(nestedResult.getTypeCode());
        return result;
      }
    } else {
      throw new AssertionError("Encountered unknown type: " + typeCode);
    }
  }

  private static void checkInitialised() {
    if (resources == null || complexTypes == null) {
      throw new IllegalStateException("Resource definitions have not been initialised");
    }
  }

  private static Map<String, SummarisedElement> summariseElements(
      @Nonnull List<ElementDefinition> elements) {
    Map<String, SummarisedElement> result = new HashMap<>();

    for (ElementDefinition element : elements) {
      List<TypeRefComponent> typeRefComponents = element.getType();
      String elementPath = element.getPath();
      assert elementPath != null : "Encountered element with no path";
      Set<String> elementChildren = findElementChildren(elementPath, elements);
      assert elementChildren != null;
      String maxCardinality = (typeRefComponents != null && !typeRefComponents.isEmpty())
          ? element.getMax()
          : null;

      // Check for elements that have multiple types - these will generate multiple entries in the
      // resulting map.
      if (elementPath.matches(".*\\[x]$")) {
        List<String> types = typeRefComponents == null
            ? new ArrayList<>()
            : typeRefComponents.stream().map(TypeRefComponent::getCode)
                .collect(Collectors.toList());
        for (String type : types) {
          assert type != null : "Encountered element type with no code";
          String transformedPath = elementPath
              .replaceAll("\\[x]", Strings.capitalize(type));
          SummarisedElement resolvedElement = new SummarisedElement(transformedPath);
          resolvedElement.setTypeCode(type);
          resolvedElement.setChildElements(elementChildren);
          resolvedElement.setMaxCardinality(maxCardinality);
          result.put(transformedPath, resolvedElement);
        }
      } else {
        String typeCode = typeRefComponents == null || typeRefComponents.isEmpty()
            ? null
            : typeRefComponents.get(0).getCode();
        SummarisedElement resolvedElement = new SummarisedElement(elementPath);
        resolvedElement.setTypeCode(typeCode);
        resolvedElement.setChildElements(elementChildren);
        resolvedElement.setMaxCardinality(maxCardinality);
        result.put(elementPath, resolvedElement);
      }
    }

    return result;
  }

  private static Set<String> findElementChildren(@Nonnull String elementPath,
      @Nonnull List<ElementDefinition> elements) {
    Set<String> children = elements.stream()
        .filter(e -> e.getPath().matches(elementPath + "\\.[a-zA-Z\\[\\]]+$"))
        .map(e -> {
          String[] pathComponents = e.getPath().split("\\.");
          if (pathComponents.length == 0) {
            throw new AssertionError("Encountered child path with no components");
          }
          return pathComponents[pathComponents.length - 1];
        })
        .collect(Collectors.toSet());
    // All instances of BackboneElement have an `id` element.
    children.add("id");
    // `extension` and `modifierExtension` are not relevant to the implementation (currently).
    children.remove("extension");
    children.remove("modifierExtension");
    return children;
  }

  public static boolean isSupportedPrimitive(String fhirType) {
    checkInitialised();
    return supportedPrimitiveTypes.contains(fhirType);
  }

  private static boolean isSupportedComplex(String fhirType) {
    checkInitialised();
    return supportedComplexTypes.contains(fhirType);
  }

  private static class SummarisedElement {

    private String path;
    private Set<String> childElements;
    @Nullable
    private String typeCode;
    @Nullable
    private String maxCardinality;

    SummarisedElement(String path) {
      this.path = path;
    }

    String getPath() {
      return path;
    }

    void setPath(String path) {
      this.path = path;
    }

    @Nullable
    String getTypeCode() {
      return typeCode;
    }

    void setTypeCode(@Nullable String typeCode) {
      this.typeCode = typeCode;
    }

    @Nullable
    String getMaxCardinality() {
      return maxCardinality;
    }

    void setMaxCardinality(@Nullable String maxCardinality) {
      this.maxCardinality = maxCardinality;
    }

    Set<String> getChildElements() {
      return childElements;
    }

    void setChildElements(Set<String> childElements) {
      this.childElements = childElements;
    }

  }

  public static class ResolvedElement {

    private String path;
    private String typeCode;
    private Map<String, Set<String>> multiValueTraversals = new HashMap<>();

    ResolvedElement(String path) {
      this.path = path;
    }

    String getPath() {
      return path;
    }

    void setPath(String path) {
      this.path = path;
    }

    public String getTypeCode() {
      return typeCode;
    }

    void setTypeCode(String typeCode) {
      this.typeCode = typeCode;
    }

    Map<String, Set<String>> getMultiValueTraversals() {
      return multiValueTraversals;
    }

    void setMultiValueTraversals(
        Map<String, Set<String>> multiValueTraversals) {
      this.multiValueTraversals = multiValueTraversals;
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
