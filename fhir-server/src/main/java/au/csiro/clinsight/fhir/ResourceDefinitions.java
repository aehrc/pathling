/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ResourceScanner.SummarisedElement;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.StructureDefinition;


/**
 * Central repository of FHIR resource complexTypes which can be looked up efficiently from across
 * the application.
 *
 * @author John Grimes
 */
public abstract class ResourceDefinitions {

  static final Set<String> supportedComplexTypes = Sets.newHashSet(
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
  static final String BASE_RESOURCE_URL_PREFIX = "http://hl7.org/fhir/StructureDefinition/";
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
  private static Map<String, Map<String, SummarisedElement>> resourceElements = new HashMap<>();
  private static Map<String, Map<String, SummarisedElement>> complexTypeElements = new HashMap<>();
  private static Map<String, StructureDefinition> resources = new HashMap<>();
  private static Map<String, StructureDefinition> complexTypes = new HashMap<>();

  /**
   * Fetches all StructureDefinitions known to the supplied terminology server, and loads them into
   * memory for later querying through the `getBaseResource` and `resolveElement` methods.
   */
  public static void ensureInitialized(@Nonnull TerminologyClient terminologyClient) {
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
      resources = ResourceScanner
          .retrieveResourceDefinitions(structureDefinitions, fetchResourceWithId);

      // Fetch each complex type StructureDefinition (just the ones that are part of the base spec)
      // and create a HashMap keyed on URL.
      complexTypes = ResourceScanner
          .retrieveComplexTypeDefinitions(structureDefinitions, fetchResourceWithId);

      // Check that all definitions have a snapshot element.
      ResourceScanner.validateDefinitions(resources.values());
      ResourceScanner.validateDefinitions(complexTypes.values());

      // Build a map of element paths and key information from their ElementDefinitions, for each
      // resource and complex type.
      resourceElements = ResourceScanner.summariseDefinitions(resources.values());
      complexTypeElements = ResourceScanner.summariseDefinitions(complexTypes.values());
    }
  }

  /**
   * Returns the StructureDefinition for a base resource type. Returns null if the resource is not
   * found.
   */
  public static @Nullable
  StructureDefinition getBaseResource(@Nonnull String resourceName) {
    checkInitialised();
    return resources.get(BASE_RESOURCE_URL_PREFIX + resourceName);
  }

  private static Map<String, SummarisedElement> getElementsForType(@Nonnull String typeName) {
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
    assert resourceOrDataType != null;
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
    assert element.getTypeCode() != null;
    assert element.getMaxCardinality() != null;
    if (!element.getMaxCardinality().equals("1")) {
      // If the current element is complex, we need to look up the children for it from the
      // element map for that type.
      if (isSupportedComplex(element.getTypeCode())) {
        SummarisedElement complexElement = getElementsForType(element.getTypeCode())
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

  public static boolean isSupportedPrimitive(@Nonnull String fhirType) {
    checkInitialised();
    return supportedPrimitiveTypes.contains(fhirType);
  }

  private static boolean isSupportedComplex(@Nonnull String fhirType) {
    checkInitialised();
    return supportedComplexTypes.contains(fhirType);
  }

  public static class ResolvedElement {

    private final Map<String, Set<String>> multiValueTraversals = new HashMap<>();
    private String path;
    private String typeCode;

    ResolvedElement(String path) {
      this.path = path;
    }

    String getPath() {
      return path;
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
