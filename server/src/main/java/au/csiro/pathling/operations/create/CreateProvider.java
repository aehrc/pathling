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

package au.csiro.pathling.operations.create;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.operations.update.UpdateExecutor;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.IResourceProvider;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that implements the FHIR create operation for a specific resource type.
 * The create operation always generates a new UUID for the resource, ignoring any client-provided
 * ID.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/http.html#create">create</a>
 */
@Component
@Scope("prototype")
@Slf4j
public class CreateProvider implements IResourceProvider {

  @Nonnull
  private final UpdateExecutor updateExecutor;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  /**
   * Constructs a new CreateProvider for a specific resource type.
   *
   * @param updateExecutor the executor for performing update operations
   * @param fhirContext the FHIR context for resource definitions
   * @param resourceClass the class of the resource type this provider handles
   */
  public CreateProvider(@Nonnull final UpdateExecutor updateExecutor,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.updateExecutor = updateExecutor;
    this.resourceClass = resourceClass;
    this.resourceType = ResourceType.fromCode(
        fhirContext.getResourceDefinition(resourceClass).getName());
  }

  @Override
  @Nonnull
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Implements the FHIR create operation. Always generates a new UUID for the resource, ignoring
   * any client-provided ID.
   *
   * @param resource the resource content to create
   * @return a MethodOutcome describing the result of the operation
   */
  @Create
  @OperationAccess("create")
  @SuppressWarnings("UnusedReturnValue")
  public MethodOutcome create(@Nullable @ResourceParam final IBaseResource resource) {
    checkUserInput(resource != null, "Resource must be supplied");

    // Always generate a new UUID, ignoring any client-provided ID.
    final String generatedId = UUID.randomUUID().toString();
    resource.setId(generatedId);

    log.debug("Creating {} with generated ID: {}", resourceType.toCode(), generatedId);
    updateExecutor.merge(resourceType, resource);

    final MethodOutcome outcome = new MethodOutcome();
    outcome.setId(new IdType(resourceType.toCode(), generatedId));
    outcome.setCreated(true);
    outcome.setResource(resource);
    return outcome;
  }

}
