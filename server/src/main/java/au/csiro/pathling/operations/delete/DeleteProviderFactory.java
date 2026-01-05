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

import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Factory for creating resource-specific DeleteProvider instances. Uses ApplicationContext to
 * create instances so that they are Spring beans and can be detected by Spring AOP for security
 * interception.
 *
 * @author John Grimes
 */
@Component
public class DeleteProviderFactory {

  @Nonnull private final ApplicationContext applicationContext;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final DeleteExecutor deleteExecutor;

  /**
   * Constructs a new DeleteProviderFactory.
   *
   * @param applicationContext the Spring application context for bean creation
   * @param fhirContext the FHIR context for resource definitions
   * @param deleteExecutor the executor for performing delete operations
   */
  public DeleteProviderFactory(
      @Nonnull final ApplicationContext applicationContext,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DeleteExecutor deleteExecutor) {
    this.applicationContext = applicationContext;
    this.fhirContext = fhirContext;
    this.deleteExecutor = deleteExecutor;
  }

  /**
   * Creates a DeleteProvider bean for the given resource type.
   *
   * @param resourceType the type of resource to create the provider for
   * @return a DeleteProvider configured for the specified resource type
   */
  @Nonnull
  public DeleteProvider createDeleteProvider(@Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass =
        fhirContext.getResourceDefinition(resourceType.name()).getImplementingClass();

    return applicationContext.getBean(
        DeleteProvider.class, deleteExecutor, fhirContext, resourceTypeClass);
  }

  /**
   * Creates a DeleteProvider bean for the given resource type code. This method supports custom
   * resource types like ViewDefinition that are not part of the standard FHIR ResourceType enum.
   *
   * @param resourceTypeCode the type code of the resource (e.g., "Patient", "ViewDefinition")
   * @return a DeleteProvider configured for the specified resource type
   */
  @Nonnull
  public DeleteProvider createDeleteProvider(@Nonnull final String resourceTypeCode) {
    final Class<? extends IBaseResource> resourceTypeClass =
        fhirContext.getResourceDefinition(resourceTypeCode).getImplementingClass();

    return applicationContext.getBean(
        DeleteProvider.class, deleteExecutor, fhirContext, resourceTypeClass);
  }
}
