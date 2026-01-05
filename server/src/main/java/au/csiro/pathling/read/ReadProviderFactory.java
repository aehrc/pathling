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

package au.csiro.pathling.read;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.stereotype.Component;

/**
 * Factory for creating resource-specific ReadProvider instances.
 *
 * @author John Grimes
 */
@Component
public class ReadProviderFactory {

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final DataSource dataSource;

  @Nonnull private final FhirEncoders fhirEncoders;

  /**
   * Constructs a new ReadProviderFactory.
   *
   * @param fhirContext the FHIR context for resource definitions
   * @param dataSource the data source containing the resources to read
   * @param fhirEncoders the encoders for converting Spark rows to FHIR resources
   */
  public ReadProviderFactory(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders) {
    this.fhirContext = fhirContext;
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
  }

  /**
   * Creates a ReadProvider for the given resource type.
   *
   * @param resourceType the type of resource to create the provider for
   * @return a ReadProvider configured for the specified resource type
   */
  @Nonnull
  public ReadProvider createReadProvider(@Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass =
        fhirContext.getResourceDefinition(resourceType.name()).getImplementingClass();

    final ReadExecutor readExecutor = new ReadExecutor(dataSource, fhirEncoders);
    return new ReadProvider(readExecutor, fhirContext, resourceTypeClass);
  }

  /**
   * Creates a ReadProvider for the given resource type code. This method supports custom resource
   * types like ViewDefinition that are not part of the standard FHIR ResourceType enum.
   *
   * @param resourceTypeCode the type code of the resource (e.g., "Patient", "ViewDefinition")
   * @return a ReadProvider configured for the specified resource type
   */
  @Nonnull
  public ReadProvider createReadProvider(@Nonnull final String resourceTypeCode) {
    final Class<? extends IBaseResource> resourceTypeClass =
        fhirContext.getResourceDefinition(resourceTypeCode).getImplementingClass();

    final ReadExecutor readExecutor = new ReadExecutor(dataSource, fhirEncoders);
    return new ReadProvider(readExecutor, fhirContext, resourceTypeClass);
  }
}
