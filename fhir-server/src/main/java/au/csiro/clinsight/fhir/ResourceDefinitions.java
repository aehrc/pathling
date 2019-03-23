/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import au.csiro.clinsight.TerminologyClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
      "Timing",
      "Reference"
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
  private static final Logger logger = LoggerFactory.getLogger(ResourceDefinitions.class);
  private static final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
      1);
  private static final long RETRY_DELAY_SECONDS = 10;
  private static Map<String, Map<String, SummarisedElement>> resourceElements = new HashMap<>();
  private static Map<String, Map<String, SummarisedElement>> complexTypeElements = new HashMap<>();
  private static Map<String, StructureDefinition> resources = new HashMap<>();
  private static Map<String, StructureDefinition> complexTypes = new HashMap<>();
  private static ResourceDefinitionsStatus status = ResourceDefinitionsStatus.UNINITIALISED;

  /**
   * Fetches all StructureDefinitions known to the supplied terminology server, and loads them into
   * memory for later querying through the `getBaseResource` and `resolveElement` methods.
   */
  public static void ensureInitialized(@Nonnull TerminologyClient terminologyClient) {
    status = ResourceDefinitionsStatus.INITIALISATION_IN_PROGRESS;
    logger.info("Initialising resource definitions...");
    try {
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

      // Success! The status can be updated to INITIALISED.
      status = ResourceDefinitionsStatus.INITIALISED;
      logger.info(resources.size() + " resource definitions and " + complexTypes.size()
          + " complex type definitions scanned");
    } catch (FhirClientConnectionException e) {
      // If there is a problem connecting to the terminology server, retry the connection using
      // progressive back off function.
      clearDefinitions();
      RetryInitialisation retryTask = new RetryInitialisation(terminologyClient);
      scheduledThreadPoolExecutor.schedule(retryTask, RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
      status = ResourceDefinitionsStatus.WAITING_FOR_RETRY;
      logger.warn("Unable to connect to terminology server, retrying in " + RETRY_DELAY_SECONDS
          + " seconds: " + terminologyClient.getServerBase(), e);
    } catch (Exception e) {
      // If there is any other sort of error, clear the state and update the status.
      clearDefinitions();
      status = ResourceDefinitionsStatus.INITIALISATION_ERROR;
      logger.error("Error initialising resource definitions", e);
    }
  }

  private static void clearDefinitions() {
    resources.clear();
    complexTypes.clear();
    resourceElements.clear();
    complexTypeElements.clear();
  }

  /**
   * Returns the StructureDefinition for a base resource type. Returns null if the resource is not
   * found.
   */
  @Nullable
  public static StructureDefinition getBaseResource(@Nonnull String resourceName) {
    checkInitialised();
    return resources.get(BASE_RESOURCE_URL_PREFIX + resourceName);
  }

  public static StructureDefinition getResourceByUrl(@Nonnull String url) {
    checkInitialised();
    return resources.get(url);
  }

  static Map<String, SummarisedElement> getElementsForType(@Nonnull String typeName) {
    Map<String, SummarisedElement> result = resourceElements
        .get(BASE_RESOURCE_URL_PREFIX + typeName);
    return result == null
        ? complexTypeElements.get(BASE_RESOURCE_URL_PREFIX + typeName)
        : result;
  }

  static void checkInitialised() {
    if (status != ResourceDefinitionsStatus.INITIALISED) {
      throw new UnclassifiedServerFailureException(503,
          "Resource definitions have not been initialised");
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

  public enum ResourceDefinitionsStatus {
    UNINITIALISED, INITIALISATION_IN_PROGRESS, WAITING_FOR_RETRY, INITIALISATION_ERROR, INITIALISED
  }

  private static class RetryInitialisation implements Runnable {

    private final TerminologyClient terminologyClient;

    RetryInitialisation(TerminologyClient terminologyClient) {
      this.terminologyClient = terminologyClient;
    }

    @Override
    public void run() {
      ResourceDefinitions.ensureInitialized(terminologyClient);
    }

  }

}
