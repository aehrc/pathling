/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.ResourceDefinitionsStatus.*;

import au.csiro.clinsight.fhir.TerminologyClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import com.google.common.collect.Sets;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.StructureDefinition;
import org.hl7.fhir.r4.model.UriType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Central repository of definitions for FHIR resources and complex types, which can be looked up
 * efficiently from across the application.
 *
 * @author John Grimes
 */
public abstract class ResourceDefinitions {

  public static final String BASE_RESOURCE_URL_PREFIX = "http://hl7.org/fhir/StructureDefinition/";
  static final Set<FHIRDefinedType> supportedComplexTypes = Sets.immutableEnumSet(
      FHIRDefinedType.RATIO,
      FHIRDefinedType.PERIOD,
      FHIRDefinedType.RANGE,
      FHIRDefinedType.ATTACHMENT,
      FHIRDefinedType.IDENTIFIER,
      FHIRDefinedType.HUMANNAME,
      FHIRDefinedType.ANNOTATION,
      FHIRDefinedType.ADDRESS,
      FHIRDefinedType.CONTACTPOINT,
      FHIRDefinedType.SAMPLEDDATA,
      FHIRDefinedType.MONEY,
      FHIRDefinedType.COUNT,
      FHIRDefinedType.DURATION,
      FHIRDefinedType.SIMPLEQUANTITY,
      FHIRDefinedType.QUANTITY,
      FHIRDefinedType.DISTANCE,
      FHIRDefinedType.AGE,
      FHIRDefinedType.CODEABLECONCEPT,
      FHIRDefinedType.SIGNATURE,
      FHIRDefinedType.CODING,
      FHIRDefinedType.TIMING,
      FHIRDefinedType.REFERENCE
  );
  static final Set<FHIRDefinedType> supportedPrimitiveTypes = Sets.immutableEnumSet(
      FHIRDefinedType.DECIMAL,
      FHIRDefinedType.MARKDOWN,
      FHIRDefinedType.ID,
      FHIRDefinedType.DATETIME,
      FHIRDefinedType.TIME,
      FHIRDefinedType.DATE,
      FHIRDefinedType.CODE,
      FHIRDefinedType.STRING,
      FHIRDefinedType.URI,
      FHIRDefinedType.OID,
      FHIRDefinedType.INTEGER,
      FHIRDefinedType.UNSIGNEDINT,
      FHIRDefinedType.POSITIVEINT,
      FHIRDefinedType.BOOLEAN,
      FHIRDefinedType.INSTANT
  );
  private static final Logger logger = LoggerFactory.getLogger(ResourceDefinitions.class);
  private static final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
      1);
  private static final long RETRY_DELAY_SECONDS = 10;
  private static Map<ResourceType, Map<String, ElementDefinition>> resourceElements = new HashMap<>();
  private static Map<FHIRDefinedType, Map<String, ElementDefinition>> complexTypeElements = new HashMap<>();
  private static Map<ResourceType, StructureDefinition> resources = new HashMap<>();
  private static Map<FHIRDefinedType, StructureDefinition> complexTypes = new HashMap<>();
  private static Set<String> knownCodeSystems = new HashSet<>();
  private static ResourceDefinitionsStatus status = UNINITIALISED;


  /**
   * Fetches all StructureDefinitions known to the supplied terminology server, and loads them into
   * memory for later querying through the `getBaseResource` and `resolvePath` methods.
   */
  public static void ensureInitialized(@Nonnull TerminologyClient terminologyClient) {
    status = INITIALISATION_IN_PROGRESS;
    logger.info("Initialising resource definitions");
    try {
      // Create a function that knows how to retrieve a StructureDefinition for a resource type.
      Function<ResourceType, StructureDefinition> fetchStrucDefForResource = resourceType -> {
        List<StructureDefinition> results = terminologyClient
            .getStructureDefinitionByUrl(
                new UriType(BASE_RESOURCE_URL_PREFIX + resourceType.toCode()));
        if (results.isEmpty()) {
          throw new UnclassifiedServerFailureException(
              500, "StructureDefinition not found for resource: " + resourceType.getDisplay());
        }
        return results.get(0);
      };

      // Create a function that knows how to retrieve a StructureDefinition for a complex type.
      Function<FHIRDefinedType, StructureDefinition> fetchStrucDefForComplexType = complexType -> {
        List<StructureDefinition> results = terminologyClient
            .getStructureDefinitionByUrl(
                new UriType(BASE_RESOURCE_URL_PREFIX + complexType.toCode()));
        if (results.isEmpty()) {
          throw new UnclassifiedServerFailureException(
              500, "StructureDefinition not found for complex type: " + complexType.getDisplay());
        }
        return results.get(0);
      };

      // Fetch each resource StructureDefinition and create a map.
      resources = ResourceDefinitions.getSupportedResources().stream()
          .peek(resourceType -> logger
              .debug("Retrieving resource StructureDefinition: " + resourceType.getDisplay()))
          .collect(Collectors.toMap(Function.identity(), fetchStrucDefForResource));

      // Fetch each complex type StructureDefinition and create a map.
      complexTypes = supportedComplexTypes.stream()
          .peek(complexType -> logger
              .debug("Retrieving complex type StructureDefinition: " + complexType.getDisplay()))
          .collect(Collectors.toMap(Function.identity(), fetchStrucDefForComplexType));

      // Check that all definitions have a snapshot element.
      ResourceScanner.validateDefinitions(resources.values());
      ResourceScanner.validateDefinitions(complexTypes.values());

      // Build a map of element paths and key information from their ElementDefinitions, for each
      // resource and complex type.
      resourceElements = ResourceScanner.summariseResourceDefinitions(resources.values());
      complexTypeElements = ResourceScanner.summariseComplexTypeDefinitions(complexTypes.values());

      // Query all known code systems from the server.
      List<CodeSystem> codeSystems = terminologyClient.getAllCodeSystems(Sets.newHashSet("url"));
      logger.debug("Querying known code systems");
      for (CodeSystem codeSystem : codeSystems) {
        knownCodeSystems.add(codeSystem.getUrl());
      }

      // Success! The status can be updated to INITIALISED.
      status = INITIALISED;
      logger.info(resources.size() + " resource definitions and " + complexTypes.size()
          + " complex type definitions scanned");
      logger.info(knownCodeSystems.size() + " code systems scanned");
    } catch (FhirClientConnectionException e) {
      // If there is a problem connecting to the terminology server, retry the connection using
      // progressive back off function.
      clearDefinitions();
      RetryInitialisation retryTask = new RetryInitialisation(terminologyClient);
      scheduledThreadPoolExecutor.schedule(retryTask, RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
      status = WAITING_FOR_RETRY;
      logger.warn("Unable to connect to terminology server, retrying in " + RETRY_DELAY_SECONDS
          + " seconds: " + terminologyClient.getServerBase(), e);
    } catch (Exception e) {
      // If there is any other sort of error, clear the state and update the status.
      clearDefinitions();
      status = INITIALISATION_ERROR;
      logger.error("Error initialising resource definitions", e);
    }
  }

  private static void clearDefinitions() {
    resources.clear();
    complexTypes.clear();
    resourceElements.clear();
    complexTypeElements.clear();
  }

  static Map<String, ElementDefinition> getElementsForResourceType(
      @Nonnull ResourceType resourceType) {
    return resourceElements.get(resourceType);
  }

  static Map<String, ElementDefinition> getElementsForComplexType(
      @Nonnull FHIRDefinedType complexType) {
    return complexTypeElements.get(complexType);
  }

  /**
   * Check if the resource definitions have been successfully initialised.
   */
  public static void checkInitialised() {
    if (status != INITIALISED) {
      throw new UnclassifiedServerFailureException(503,
          "Resource definitions have not been initialised");
    }
  }

  /**
   * Return the set of supported resource types.
   */
  public static Set<ResourceType> getSupportedResources() {
    return Arrays.stream(ResourceType.values())
        .filter(resourceType -> !resourceType.equals(ResourceType.NULL))
        .collect(Collectors.toSet());
  }

  /**
   * Check if the supplied resource name is a supported resource.
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static boolean isSupportedResourceName(String resourceName) {
    try {
      ResourceType.fromCode(resourceName);
    } catch (FHIRException e) {
      return false;
    }
    return true;
  }

  /**
   * Check if the supplied FHIR type code corresponds to a supported primitive type.
   */
  public static boolean isPrimitive(@Nonnull FHIRDefinedType fhirType) {
    checkInitialised();
    return supportedPrimitiveTypes.contains(fhirType);
  }

  /**
   * Check if the supplied string is the URL of a known CodeSystem.
   */
  public static boolean isCodeSystemKnown(@Nonnull String url) {
    return knownCodeSystems.contains(url);
  }

  public enum ResourceDefinitionsStatus {
    UNINITIALISED, INITIALISATION_IN_PROGRESS, WAITING_FOR_RETRY, INITIALISATION_ERROR, INITIALISED
  }

  /**
   * A runnable task that is used for scheduled retry of the requests to the terminology server.
   */
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
