/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Update;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.IResourceProvider;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that implements the FHIR update operation for a specific resource type.
 * Supports both standard FHIR resource types and custom resource types like ViewDefinition.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/http.html#update">update</a>
 */
@Component
@Scope("prototype")
@Slf4j
public class UpdateProvider implements IResourceProvider {

  @Nonnull
  private final UpdateExecutor updateExecutor;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final String resourceTypeCode;

  /**
   * Constructs a new UpdateProvider for a specific resource type.
   *
   * @param updateExecutor the executor for performing update operations
   * @param fhirContext the FHIR context for resource definitions
   * @param resourceClass the class of the resource type this provider handles
   */
  public UpdateProvider(@Nonnull final UpdateExecutor updateExecutor,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.updateExecutor = updateExecutor;
    this.resourceClass = resourceClass;
    this.resourceTypeCode = fhirContext.getResourceDefinition(resourceClass).getName();
  }

  @Override
  @Nonnull
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Implements the FHIR update operation.
   *
   * @param id the ID of the resource to update
   * @param resource the resource content to update
   * @return a MethodOutcome describing the result of the operation
   */
  @Update
  @OperationAccess("update")
  @SuppressWarnings("UnusedReturnValue")
  public MethodOutcome update(@Nullable @IdParam final IdType id,
      @Nullable @ResourceParam final IBaseResource resource) {
    checkUserInput(id != null && !id.isEmpty(), "ID must be supplied");
    checkUserInput(resource != null, "Resource must be supplied");

    final String resourceId = id.getIdPart();
    final IBaseResource preparedResource = prepareResourceForUpdate(resource, resourceId);

    log.debug("Updating {} with ID: {}", resourceTypeCode, resourceId);
    updateExecutor.merge(resourceTypeCode, preparedResource);

    final MethodOutcome outcome = new MethodOutcome();
    outcome.setId(resource.getIdElement());
    outcome.setResource(preparedResource);
    return outcome;
  }

}
