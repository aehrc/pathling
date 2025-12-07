/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that can be instantiated using any resource type to add the FHIRPath
 * search functionality.
 *
 * @author John Grimes
 */
@Component
@Scope("prototype")
@Slf4j
public class SearchProvider implements IResourceProvider {

  /**
   * The name of the FHIR search profile.
   */
  private static final String QUERY_NAME = "fhirPath";

  /**
   * The name of the parameter which is passed to the search profile.
   */
  private static final String FILTER_PARAM = "filter";

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  /**
   * Constructs a new SearchProvider.
   *
   * @param configuration the server configuration
   * @param fhirContext the FHIR context for FHIR operations
   * @param dataSource the data source containing the resources to query
   * @param fhirEncoders the encoders for converting Spark rows to FHIR resources
   * @param resourceClass the class of the resource type to search
   */
  public SearchProvider(@Nonnull final ServerConfiguration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
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
   * Handles all search requests for the resource of the nominated type with no filters.
   *
   * @return a {@link SearchExecutor} which will generate the Bundle of results
   */
  @Search
  @OperationAccess("search")
  @SuppressWarnings("UnusedReturnValue")
  public IBundleProvider search() {
    return buildSearchExecutor(Optional.empty());
  }

  /**
   * Handles all search requests for the resource of the nominated type that contain filters.
   *
   * @param filters the AND/OR search parameters passed using the "filter" key
   * @return a {@link SearchExecutor} which will generate the Bundle of results
   */
  @Search(queryName = QUERY_NAME)
  @OperationAccess("search")
  @SuppressWarnings("UnusedReturnValue")
  public IBundleProvider search(
      @Nullable @OptionalParam(name = FILTER_PARAM) final StringAndListParam filters) {
    return buildSearchExecutor(Optional.ofNullable(filters));
  }

  @Nonnull
  private IBundleProvider buildSearchExecutor(
      @Nonnull final Optional<StringAndListParam> filters) {
    final boolean cacheResults = configuration.getQuery().getCacheResults();
    return new SearchExecutor(fhirContext, dataSource, fhirEncoders, resourceType, filters,
        cacheResults);
  }

}
