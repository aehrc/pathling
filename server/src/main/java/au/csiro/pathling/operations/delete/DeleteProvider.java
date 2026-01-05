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

package au.csiro.pathling.operations.delete;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Delete;
import ca.uhn.fhir.rest.annotation.IdParam;
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
 * HAPI resource provider that implements the FHIR delete operation for a specific resource type.
 * Supports both standard FHIR resource types and custom resource types like ViewDefinition.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/http.html#delete">delete</a>
 */
@Component
@Scope("prototype")
@Slf4j
public class DeleteProvider implements IResourceProvider {

  @Nonnull private final DeleteExecutor deleteExecutor;

  @Nonnull private final Class<? extends IBaseResource> resourceClass;

  @Nonnull private final String resourceTypeCode;

  /**
   * Constructs a new DeleteProvider for a specific resource type.
   *
   * @param deleteExecutor the executor for performing delete operations
   * @param fhirContext the FHIR context for resource definitions
   * @param resourceClass the class of the resource type this provider handles
   */
  public DeleteProvider(
      @Nonnull final DeleteExecutor deleteExecutor,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.deleteExecutor = deleteExecutor;
    this.resourceClass = resourceClass;
    this.resourceTypeCode = fhirContext.getResourceDefinition(resourceClass).getName();
  }

  @Override
  @Nonnull
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Implements the FHIR delete operation.
   *
   * @param id the ID of the resource to delete
   * @return a MethodOutcome describing the result of the operation
   */
  @Delete
  @OperationAccess("delete")
  @SuppressWarnings("UnusedReturnValue")
  public MethodOutcome delete(@Nullable @IdParam final IdType id) {
    checkUserInput(id != null && !id.isEmpty(), "ID must be supplied");

    final String resourceId = id.getIdPart();
    log.debug("Deleting {} with ID: {}", resourceTypeCode, resourceId);
    deleteExecutor.delete(resourceTypeCode, resourceId);

    final MethodOutcome outcome = new MethodOutcome();
    outcome.setId(id);
    return outcome;
  }
}
