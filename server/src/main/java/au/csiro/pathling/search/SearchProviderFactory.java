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

package au.csiro.pathling.search;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * A factory that creates resource-specific SearchProvider instances. It uses ApplicationContext to
 * create the instances so that they are Spring beans and can be detected by Spring AOP for security
 * interception.
 *
 * @author John Grimes
 */
@Component
public class SearchProviderFactory {

  @Nonnull private final ApplicationContext applicationContext;

  @Nonnull private final ServerConfiguration configuration;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final QueryableDataSource dataSource;

  @Nonnull private final FhirEncoders fhirEncoders;

  /**
   * Constructs a new SearchProviderFactory.
   *
   * @param applicationContext the Spring application context for bean creation
   * @param configuration the server configuration
   * @param fhirContext the FHIR context for FHIR operations
   * @param dataSource the data source containing the resources to query
   * @param fhirEncoders the encoders for converting Spark rows to FHIR resources
   */
  public SearchProviderFactory(
      @Nonnull final ApplicationContext applicationContext,
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final QueryableDataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders) {
    this.applicationContext = applicationContext;
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
  }

  /**
   * Creates a SearchProvider bean for the given resource type.
   *
   * @param resourceType the type of resource to create the provider for
   * @return a SearchProvider configured for the specified resource type
   */
  @Nonnull
  public SearchProvider createSearchProvider(@Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass =
        fhirContext.getResourceDefinition(resourceType.name()).getImplementingClass();

    return applicationContext.getBean(
        SearchProvider.class,
        configuration,
        fhirContext,
        dataSource,
        fhirEncoders,
        resourceTypeClass);
  }

  /**
   * Creates a SearchProvider bean for the given resource type code. This method supports custom
   * resource types like ViewDefinition that are not part of the standard FHIR ResourceType enum.
   *
   * @param resourceTypeCode the type code of the resource (e.g., "Patient", "ViewDefinition")
   * @return a SearchProvider configured for the specified resource type
   */
  @Nonnull
  public SearchProvider createSearchProvider(@Nonnull final String resourceTypeCode) {
    final Class<? extends IBaseResource> resourceTypeClass =
        fhirContext.getResourceDefinition(resourceTypeCode).getImplementingClass();

    return applicationContext.getBean(
        SearchProvider.class,
        configuration,
        fhirContext,
        dataSource,
        fhirEncoders,
        resourceTypeClass);
  }
}
