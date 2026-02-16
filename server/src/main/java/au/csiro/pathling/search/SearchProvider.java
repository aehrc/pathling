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
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.RawParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that can be instantiated using any resource type to add search
 * functionality. Supports both standard FHIR search parameters and FHIRPath-based search via the
 * {@code _query=fhirPath} named query.
 *
 * @author John Grimes
 */
@Component
@Scope("prototype")
@Slf4j
public class SearchProvider implements IResourceProvider {

  /** The name of the FHIR search profile. */
  private static final String QUERY_NAME = "fhirPath";

  /** The name of the parameter which is passed to the search profile. */
  private static final String FILTER_PARAM = "filter";

  @Nonnull private final ServerConfiguration configuration;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final DataSource dataSource;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final Class<? extends IBaseResource> resourceClass;

  @Nonnull private final String resourceTypeCode;

  /**
   * Constructs a new SearchProvider.
   *
   * @param configuration the server configuration
   * @param fhirContext the FHIR context for FHIR operations
   * @param dataSource the data source containing the resources to query
   * @param fhirEncoders the encoders for converting Spark rows to FHIR resources
   * @param resourceClass the class of the resource type to search
   */
  public SearchProvider(
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
    this.resourceClass = resourceClass;
    this.resourceTypeCode = fhirContext.getResourceDefinition(resourceClass).getName();
  }

  @Override
  @Nonnull
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Handles search requests with standard FHIR search parameters. Unknown parameters are accepted
   * via {@code @RawParam} and passed through to the SearchExecutor for processing.
   *
   * @param rawParams the raw search parameters from the URL query string, or null if none
   * @return a {@link SearchExecutor} which will generate the Bundle of results
   */
  @Search(allowUnknownParams = true)
  @OperationAccess("search")
  @SuppressWarnings("UnusedReturnValue")
  public IBundleProvider search(@Nullable @RawParam final Map<String, List<String>> rawParams) {
    final String queryString = buildQueryString(rawParams);
    return buildSearchExecutor(queryString, Optional.empty());
  }

  /**
   * Handles search requests using the FHIRPath named query, optionally combined with standard FHIR
   * search parameters.
   *
   * @param filters the AND/OR FHIRPath filter expressions passed using the "filter" key
   * @param rawParams the raw standard search parameters from the URL query string, or null if none
   * @return a {@link SearchExecutor} which will generate the Bundle of results
   */
  @Search(queryName = QUERY_NAME, allowUnknownParams = true)
  @OperationAccess("search")
  @SuppressWarnings("UnusedReturnValue")
  public IBundleProvider search(
      @Nullable @OptionalParam(name = FILTER_PARAM) final StringAndListParam filters,
      @Nullable @RawParam final Map<String, List<String>> rawParams) {
    final String queryString = buildQueryString(rawParams);
    return buildSearchExecutor(queryString, Optional.ofNullable(filters));
  }

  /**
   * Reconstructs a query string from the raw parameter map. Each key-value pair is joined with
   * {@code =}, and pairs are joined with {@code &}. Multi-valued entries (repeated URL parameters)
   * emit separate {@code key=value} pairs for each value. Modifier suffixes in map keys (e.g.,
   * {@code gender:not}) are preserved as-is.
   *
   * @param rawParams the raw parameter map, or null if no parameters
   * @return the reconstructed query string, or null if the map is null or empty
   */
  @Nullable
  static String buildQueryString(@Nullable final Map<String, List<String>> rawParams) {
    if (rawParams == null || rawParams.isEmpty()) {
      return null;
    }
    return rawParams.entrySet().stream()
        .flatMap(entry -> entry.getValue().stream().map(value -> entry.getKey() + "=" + value))
        .collect(Collectors.joining("&"));
  }

  @Nonnull
  private IBundleProvider buildSearchExecutor(
      @Nullable final String standardSearchQueryString,
      @Nonnull final Optional<StringAndListParam> filters) {
    final boolean cacheResults = configuration.getQuery().isCacheResults();
    return new SearchExecutor(
        fhirContext,
        dataSource,
        fhirEncoders,
        resourceTypeCode,
        standardSearchQueryString,
        filters,
        cacheResults);
  }
}
