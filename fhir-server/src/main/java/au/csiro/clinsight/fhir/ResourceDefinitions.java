/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ResourceScanner.SummarisedElement;
import com.google.common.collect.Sets;
import java.util.HashMap;
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
  static final Set<String> supportedPrimitiveTypes = Sets.newHashSet(
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
      Function<StructureDefinition, StructureDefinition> fetchResourceWithId = definition -> terminologyClient
          .getStructureDefinitionById(new IdType(definition.getId()));

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

  static Map<String, SummarisedElement> getElementsForType(@Nonnull String typeName) {
    Map<String, SummarisedElement> result = resourceElements
        .get(BASE_RESOURCE_URL_PREFIX + typeName);
    return result == null
        ? complexTypeElements.get(BASE_RESOURCE_URL_PREFIX + typeName)
        : result;
  }

  static void checkInitialised() {
    if (resources == null || complexTypes == null) {
      throw new IllegalStateException("Resource definitions have not been initialised");
    }
  }

  public static boolean isPrimitive(@Nonnull String fhirType) {
    checkInitialised();
    return supportedPrimitiveTypes.contains(fhirType);
  }

  public static boolean isComplex(@Nonnull String fhirType) {
    checkInitialised();
    return supportedComplexTypes.contains(fhirType);
  }

  public static boolean isResource(@Nonnull String fhirType) {
    char firstChar = fhirType.charAt(0);
    return Character.isUpperCase(firstChar);
  }

  public static boolean isBackboneElement(@Nonnull String fhirType) {
    return fhirType.equals("BackboneElement");
  }

}
