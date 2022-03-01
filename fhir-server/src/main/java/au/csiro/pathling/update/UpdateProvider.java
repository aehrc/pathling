/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.IResourceProvider;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that provides update-related operations, such as create, update and
 * transaction.
 *
 * @author John Grimes
 */
@Component
@Scope("prototype")
@Profile("server")
public class UpdateProvider implements IResourceProvider {

  @Nonnull
  private final UpdateHelpers updateHelpers;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  @Nonnull
  private final CacheInvalidator cacheInvalidator;

  /**
   * @param resourceClass the resource class that this provider will receive requests for
   */
  public UpdateProvider(@Nonnull final UpdateHelpers updateHelpers,
      @Nonnull final CacheInvalidator cacheInvalidator,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.updateHelpers = updateHelpers;
    this.resourceClass = resourceClass;
    this.cacheInvalidator = cacheInvalidator;
    resourceType = resourceTypeFromClass(resourceClass);
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  @Create
  @OperationAccess("create")
  public MethodOutcome create(@ResourceParam final IBaseResource resource) {
    resource.setId(UUID.randomUUID().toString());

    updateHelpers.appendDataset(resourceType, resource);

    cacheInvalidator.invalidateAll();

    final MethodOutcome outcome = new MethodOutcome();
    outcome.setId(resource.getIdElement());
    outcome.setResource(resource);
    return outcome;
  }

  @Update
  @OperationAccess("update")
  public MethodOutcome update(@IdParam final IdType id,
      @ResourceParam final IBaseResource resource) {
    String resourceId = id.getIdPart();
    resource.setId(resourceId);

    updateHelpers.updateDataset(resourceType, resource);

    cacheInvalidator.invalidateAll();

    final MethodOutcome outcome = new MethodOutcome();
    outcome.setId(resource.getIdElement());
    outcome.setResource(resource);
    return outcome;
  }
}
