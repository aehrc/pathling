package au.csiro.pathling.update;

import static au.csiro.pathling.update.UpdateProvider.prepareResourceForCreate;
import static au.csiro.pathling.update.UpdateProvider.prepareResourceForUpdate;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.hl7.fhir.r4.model.Bundle.BundleType.BATCHRESPONSE;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.FhirServer;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Resource;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * HAPI plain provider that provides update operations with a system-wide batch submission as a
 * bundle.
 *
 * @author Sean Fong
 */
@Component
@Profile("server")
public class BatchProvider {

  @Nonnull
  private final UpdateHelpers updateHelpers;

  @Nonnull
  private final CacheInvalidator cacheInvalidator;

  private static final Pattern CREATE_URL = Pattern.compile("^[A-Za-z]+$");
  @SuppressWarnings("RegExpRedundantEscape")
  private static final Pattern UPDATE_URL = Pattern.compile(
      "^[A-Za-z]+/[A-Za-z0-9\\-\\.&&[^\\$]][A-Za-z0-9\\-\\.]{0,63}$");

  public BatchProvider(@Nonnull final UpdateHelpers updateHelpers,
      @Nonnull final CacheInvalidator cacheInvalidator) {
    this.updateHelpers = updateHelpers;
    this.cacheInvalidator = cacheInvalidator;
  }

  @Transaction
  @OperationAccess("batch")
  public Bundle batch(@Nullable @TransactionParam final Bundle bundle) {
    checkUserInput(bundle != null, "Bundle must be provided");

    final Map<ResourceType, List<IBaseResource>> resourcesForCreation = new EnumMap<>(
        ResourceType.class);
    final Map<ResourceType, List<IBaseResource>> resourcesForUpdate = new EnumMap<>(
        ResourceType.class);
    final Bundle response = new Bundle();
    response.setType(BATCHRESPONSE);

    // Gather all the resources within the request bundle, categorised by their type and whether it 
    // is an update or a create operation. Also, prepare the responses should these operations be 
    // successful.
    for (final Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      final Resource resource = entry.getResource();
      final BundleEntryRequestComponent request = entry.getRequest();
      checkUserInput(resource != null,
          "Each batch entry must have a resource element");
      checkUserInput(request != null && !request.isEmpty(),
          "Each batch entry must have a request element");
      final boolean postRequest = checkRequestType(request, "POST", CREATE_URL);
      final boolean putRequest = checkRequestType(request, "PUT", UPDATE_URL);
      if (postRequest) {
        final String urlErrorMessage =
            "The URL for a create request must be equal to the code of a "
                + "supported resource type";
        final ResourceType resourceType = validateResourceType(entry, request.getUrl(),
            urlErrorMessage);
        final IBaseResource preparedResource = prepareResourceForCreate(resource);
        addToResourceMap(resourcesForCreation, resourceType, preparedResource);
        addResponse(response, preparedResource);
      } else if (putRequest) {
        final String urlErrorMessage = "The URL for an update request must refer to the code of a "
            + "supported resource type, and must look like this: [resource type]/[id]";
        final List<String> urlComponents = List.of(request.getUrl().split("/"));
        checkUserInput(urlComponents.size() == 2, urlErrorMessage);
        final String resourceTypeCode = urlComponents.get(0);
        final String urlId = urlComponents.get(1);
        final ResourceType resourceType = validateResourceType(entry, resourceTypeCode,
            urlErrorMessage);
        final IBaseResource preparedResource = prepareResourceForUpdate(resource, urlId);
        addToResourceMap(resourcesForUpdate, resourceType, preparedResource);
        addResponse(response, preparedResource);
      } else {
        throw new InvalidUserInputError(
            "Only create and update operations are supported via batch");
      }
    }

    // Append any newly created resources to their respective tables.
    for (final ResourceType resourceType : resourcesForCreation.keySet()) {
      updateHelpers.appendDataset(resourceType, resourcesForCreation.get(resourceType));
    }

    // Merge in any updated resources into their respective tables.
    for (final ResourceType resourceType : resourcesForUpdate.keySet()) {
      updateHelpers.updateDataset(resourceType, resourcesForUpdate.get(resourceType));
    }

    cacheInvalidator.invalidateAll();
    return response;
  }

  private boolean checkRequestType(@Nonnull final BundleEntryRequestComponent request,
      @Nonnull final String method, @Nonnull final Pattern urlPattern) {
    return request.getMethod().toString().equals(method) &&
        urlPattern.matcher(request.getUrl()).matches();
  }

  @Nonnull
  private ResourceType validateResourceType(final BundleEntryComponent entry,
      final String resourceTypeCode, final String urlErrorMessage) {
    final ResourceType resourceType;
    try {
      resourceType = ResourceType.fromCode(resourceTypeCode);
    } catch (final FHIRException e) {
      throw new InvalidUserInputError(urlErrorMessage);
    }
    checkUserInput(FhirServer.supportedResourceTypes().contains(resourceType),
        urlErrorMessage);
    final String resourceEmbeddedType = entry.getResource().fhirType();
    checkUserInput(resourceTypeCode.equals(resourceEmbeddedType),
        "Resource in URL does not match resource type");
    return resourceType;
  }

  private void addToResourceMap(
      @Nonnull final Map<ResourceType, List<IBaseResource>> resourcesForCreation,
      @Nonnull final ResourceType resourceType, @Nonnull final IBaseResource resource) {
    resourcesForCreation.computeIfAbsent(resourceType, k -> new ArrayList<>());
    resourcesForCreation.get(resourceType).add(resource);
  }

  private void addResponse(@Nonnull final Bundle response,
      @Nonnull final IBaseResource preparedResource) {
    final BundleEntryComponent responseEntry = response.addEntry();
    final BundleEntryResponseComponent responseElement = new BundleEntryResponseComponent();
    responseElement.setStatus("200");
    responseEntry.setResponse(responseElement);
    responseEntry.setResource((Resource) preparedResource);
  }

}
