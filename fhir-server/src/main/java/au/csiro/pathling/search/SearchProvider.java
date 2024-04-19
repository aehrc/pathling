/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
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
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
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
@Profile("server")
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
  private final SparkSession sparkSession;

  @Nonnull
  private final Database database;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  /**
   * @param configuration A {@link ServerConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param database A {@link Database} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param resourceClass A Class that extends {@link IBaseResource} that represents the type of
   * resource to be searched
   */
  public SearchProvider(@Nonnull final ServerConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final Database database,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.database = database;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.fhirEncoders = fhirEncoders;
    this.resourceClass = resourceClass;
    resourceType = resourceTypeFromClass(resourceClass);
  }

  @Override
  @Nonnull
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  /**
   * Handles all search requests for the resource of the nominated type with no filters.
   *
   * @return A {@link SearchExecutor} which will generate the {@link org.hl7.fhir.r4.model.Bundle}
   * of results
   */
  @Search
  @OperationAccess("search")
  @SuppressWarnings({"UnusedReturnValue"})
  public IBundleProvider search() {
    final ResourceType subjectResource = ResourceDefinition.getResourceTypeFromClass(resourceClass);
    return buildSearchExecutor(subjectResource, Optional.empty());
  }

  /**
   * Handles all search requests for the resource of the nominated type that contain filters.
   *
   * @param filters The AND/OR search parameters passed using the "filter" key
   * @return A {@link SearchExecutor} which will generate the {@link org.hl7.fhir.r4.model.Bundle}
   * of results
   */
  @Search(queryName = QUERY_NAME)
  @OperationAccess("search")
  @SuppressWarnings({"UnusedReturnValue"})
  public IBundleProvider search(
      @Nullable @OptionalParam(name = FILTER_PARAM) final StringAndListParam filters) {
    return buildSearchExecutor(resourceType, Optional.ofNullable(filters));
  }

  @Nonnull
  private IBundleProvider buildSearchExecutor(@Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<StringAndListParam> filters) {
    return new SearchExecutor(configuration.getQuery(), fhirContext, sparkSession, database,
        terminologyServiceFactory, fhirEncoders, subjectResource, filters);
  }

}
