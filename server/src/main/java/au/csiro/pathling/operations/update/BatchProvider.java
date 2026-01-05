/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.operations.update;

import static au.csiro.pathling.operations.update.UpdateExecutor.prepareResourceForUpdate;
import static au.csiro.pathling.security.SecurityAspect.checkHasAuthority;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.hl7.fhir.r4.model.Bundle.BundleType.BATCHRESPONSE;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.operations.delete.DeleteExecutor;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.security.PathlingAuthority;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Resource;
import org.springframework.stereotype.Component;

/**
 * HAPI provider that implements the FHIR batch operation for create and update operations.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/http.html#transaction">batch/transaction</a>
 */
@Component
@Slf4j
public class BatchProvider {

  @Nonnull private final UpdateExecutor updateExecutor;

  @Nonnull private final DeleteExecutor deleteExecutor;

  @Nonnull private final ServerConfiguration configuration;

  @SuppressWarnings("RegExpRedundantEscape")
  private static final Pattern UPDATE_URL =
      Pattern.compile("^[A-Za-z]+/[A-Za-z0-9\\-\\.&&[^\\$]][A-Za-z0-9\\-\\.]{0,63}$");

  private static final Pattern CREATE_URL = Pattern.compile("^[A-Za-z]+$");

  /**
   * Constructs a new BatchProvider.
   *
   * @param updateExecutor the executor for performing update operations
   * @param deleteExecutor the executor for performing delete operations
   * @param configuration the server configuration
   */
  public BatchProvider(
      @Nonnull final UpdateExecutor updateExecutor,
      @Nonnull final DeleteExecutor deleteExecutor,
      @Nonnull final ServerConfiguration configuration) {
    this.updateExecutor = updateExecutor;
    this.deleteExecutor = deleteExecutor;
    this.configuration = configuration;
  }

  /**
   * Implements the FHIR batch operation.
   *
   * @param bundle the batch Bundle containing the update requests
   * @return a batch response Bundle with the status of each entry
   */
  @Transaction
  @OperationAccess("batch")
  public Bundle batch(@Nullable @TransactionParam final Bundle bundle) {
    checkUserInput(bundle != null, "Bundle must be provided");

    final Map<ResourceType, List<IBaseResource>> resourcesForUpdate =
        new EnumMap<>(ResourceType.class);
    final Bundle response = new Bundle();
    response.setType(BATCHRESPONSE);

    // Gather all the resources within the request bundle, categorised by their type. Also, prepare
    // the responses should these operations be successful.
    for (final BundleEntryComponent entry : bundle.getEntry()) {
      processEntry(resourcesForUpdate, response, entry);
    }

    if (!resourcesForUpdate.isEmpty()) {
      // Merge in any updated resources into their respective tables.
      update(resourcesForUpdate);
    }

    return response;
  }

  private void processEntry(
      @Nonnull final Map<ResourceType, List<IBaseResource>> resourcesForUpdate,
      @Nonnull final Bundle response,
      @Nonnull final BundleEntryComponent entry) {
    final Resource resource = entry.getResource();
    final BundleEntryRequestComponent request = entry.getRequest();
    if (request == null || request.isEmpty()) {
      return;
    }

    final String method = request.getMethod().toString();
    if ("POST".equals(method)) {
      if (resource == null) {
        return;
      }
      processCreateEntry(resourcesForUpdate, response, entry);
    } else if ("PUT".equals(method)) {
      if (resource == null) {
        return;
      }
      processUpdateEntry(resourcesForUpdate, response, entry);
    } else if ("DELETE".equals(method)) {
      processDeleteEntry(response, entry);
    } else {
      throw new InvalidUserInputError(
          "Only create (POST), update (PUT), and delete (DELETE) requests are supported within "
              + "the batch operation");
    }
  }

  private void processCreateEntry(
      @Nonnull final Map<ResourceType, List<IBaseResource>> resourcesForUpdate,
      @Nonnull final Bundle response,
      @Nonnull final BundleEntryComponent entry) {
    final Resource resource = entry.getResource();
    final BundleEntryRequestComponent request = entry.getRequest();

    final String urlErrorMessage =
        "The URL for a create request must refer to the code of a "
            + "supported resource type, and must look like this: [resource type]";
    checkUserInput(CREATE_URL.matcher(request.getUrl()).matches(), urlErrorMessage);

    final String resourceTypeCode = request.getUrl();
    final ResourceType resourceType =
        validateResourceTypeForCreate(entry, resourceTypeCode, urlErrorMessage);

    // Generate a new UUID, ignoring any client-provided ID.
    final String generatedId = UUID.randomUUID().toString();
    resource.setId(generatedId);

    log.debug("Batch creating {} with generated ID: {}", resourceType.toCode(), generatedId);
    addToResourceMap(resourcesForUpdate, resourceType, resource);
    addCreateResponse(response, resource);
  }

  private void processUpdateEntry(
      @Nonnull final Map<ResourceType, List<IBaseResource>> resourcesForUpdate,
      @Nonnull final Bundle response,
      @Nonnull final BundleEntryComponent entry) {
    final Resource resource = entry.getResource();
    final BundleEntryRequestComponent request = entry.getRequest();

    final String urlErrorMessage =
        "The URL for an update request must refer to the code of a "
            + "supported resource type, and must look like this: [resource type]/[id]";
    checkUserInput(UPDATE_URL.matcher(request.getUrl()).matches(), urlErrorMessage);
    final List<String> urlComponents = List.of(request.getUrl().split("/"));
    checkUserInput(urlComponents.size() == 2, urlErrorMessage);
    final String resourceTypeCode = urlComponents.get(0);
    final String urlId = urlComponents.get(1);
    final ResourceType resourceType =
        validateResourceType(entry, resourceTypeCode, urlErrorMessage);
    final IBaseResource preparedResource = prepareResourceForUpdate(resource, urlId);
    addToResourceMap(resourcesForUpdate, resourceType, preparedResource);
    addUpdateResponse(response, preparedResource);
  }

  private void processDeleteEntry(
      @Nonnull final Bundle response, @Nonnull final BundleEntryComponent entry) {
    final BundleEntryRequestComponent request = entry.getRequest();

    final String urlErrorMessage =
        "The URL for a delete request must refer to the code of a "
            + "supported resource type, and must look like this: [resource type]/[id]";
    checkUserInput(UPDATE_URL.matcher(request.getUrl()).matches(), urlErrorMessage);
    final List<String> urlComponents = List.of(request.getUrl().split("/"));
    checkUserInput(urlComponents.size() == 2, urlErrorMessage);
    final String resourceTypeCode = urlComponents.get(0);
    final String resourceId = urlComponents.get(1);
    validateResourceTypeForDelete(resourceTypeCode, urlErrorMessage);

    if (configuration.getAuth().isEnabled()) {
      checkHasAuthority(PathlingAuthority.operationAccess("delete"));
    }

    log.debug("Batch deleting {} with ID: {}", resourceTypeCode, resourceId);
    deleteExecutor.delete(resourceTypeCode, resourceId);
    addDeleteResponse(response);
  }

  private void update(@Nonnull final Map<ResourceType, List<IBaseResource>> resourcesForUpdate) {
    if (configuration.getAuth().isEnabled()) {
      checkHasAuthority(PathlingAuthority.operationAccess("update"));
    }
    for (final ResourceType resourceType : resourcesForUpdate.keySet()) {
      log.debug(
          "Batch updating {} resource(s) of type {}",
          resourcesForUpdate.get(resourceType).size(),
          resourceType.toCode());
      updateExecutor.merge(resourceType, resourcesForUpdate.get(resourceType));
    }
  }

  @Nonnull
  private ResourceType validateResourceType(
      @Nonnull final BundleEntryComponent entry,
      @Nonnull final String resourceTypeCode,
      @Nonnull final String urlErrorMessage) {
    final ResourceType resourceType;
    try {
      resourceType = ResourceType.fromCode(resourceTypeCode);
    } catch (final FHIRException e) {
      throw new InvalidUserInputError(urlErrorMessage);
    }
    checkUserInput(FhirServer.supportedResourceTypes().contains(resourceType), urlErrorMessage);
    final String resourceEmbeddedType = entry.getResource().fhirType();
    checkUserInput(
        resourceTypeCode.equals(resourceEmbeddedType),
        "Resource in URL does not match resource type");
    return resourceType;
  }

  @Nonnull
  private ResourceType validateResourceTypeForCreate(
      @Nonnull final BundleEntryComponent entry,
      @Nonnull final String resourceTypeCode,
      @Nonnull final String urlErrorMessage) {
    final ResourceType resourceType;
    try {
      resourceType = ResourceType.fromCode(resourceTypeCode);
    } catch (final FHIRException e) {
      throw new InvalidUserInputError(urlErrorMessage);
    }
    checkUserInput(FhirServer.supportedResourceTypes().contains(resourceType), urlErrorMessage);
    final String resourceEmbeddedType = entry.getResource().fhirType();
    checkUserInput(
        resourceTypeCode.equals(resourceEmbeddedType),
        "Resource type in URL does not match resource type in body");
    return resourceType;
  }

  private void validateResourceTypeForDelete(
      @Nonnull final String resourceTypeCode, @Nonnull final String urlErrorMessage) {
    final ResourceType resourceType;
    try {
      resourceType = ResourceType.fromCode(resourceTypeCode);
    } catch (final FHIRException e) {
      throw new InvalidUserInputError(urlErrorMessage);
    }
    checkUserInput(FhirServer.supportedResourceTypes().contains(resourceType), urlErrorMessage);
  }

  private void addToResourceMap(
      @Nonnull final Map<ResourceType, List<IBaseResource>> resourcesForCreation,
      @Nonnull final ResourceType resourceType,
      @Nonnull final IBaseResource resource) {
    resourcesForCreation.computeIfAbsent(resourceType, k -> new ArrayList<>());
    resourcesForCreation.get(resourceType).add(resource);
  }

  private void addCreateResponse(
      @Nonnull final Bundle response, @Nonnull final IBaseResource createdResource) {
    final BundleEntryComponent responseEntry = response.addEntry();
    final BundleEntryResponseComponent responseElement = new BundleEntryResponseComponent();
    responseElement.setStatus("201");
    responseEntry.setResponse(responseElement);
    responseEntry.setResource((Resource) createdResource);
  }

  private void addUpdateResponse(
      @Nonnull final Bundle response, @Nonnull final IBaseResource updatedResource) {
    final BundleEntryComponent responseEntry = response.addEntry();
    final BundleEntryResponseComponent responseElement = new BundleEntryResponseComponent();
    responseElement.setStatus("200");
    responseEntry.setResponse(responseElement);
    responseEntry.setResource((Resource) updatedResource);
  }

  private void addDeleteResponse(@Nonnull final Bundle response) {
    final BundleEntryComponent responseEntry = response.addEntry();
    final BundleEntryResponseComponent responseElement = new BundleEntryResponseComponent();
    responseElement.setStatus("204");
    responseEntry.setResponse(responseElement);
  }
}
