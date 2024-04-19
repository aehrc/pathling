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

package au.csiro.pathling.fhir;

import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.extract.ExtractExecutor;
import au.csiro.pathling.extract.ExtractProvider;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.search.SearchProvider;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.update.UpdateProvider;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.IResourceProvider;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ResourceType;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * A factory that encapsulates creation of resource specific ResourceProviders. It uses
 * ApplicationContext to create the instances of ResourceProviders so that they are spring beans and
 * as such can be detected by SpringAOP (which is required for the security implementation).
 */
@Component
@Profile("server")
public class ResourceProviderFactory {

  @Nonnull
  private final ApplicationContext applicationContext;

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private final AggregateExecutor aggregateExecutor;

  @Nonnull
  private final ExtractExecutor extractExecutor;

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

  /**
   * @param applicationContext the Spring {@link ApplicationContext}
   * @param fhirContext a {@link FhirContext} for doing FHIR stuff
   * @param configuration a {@link ServerConfiguration} instance which controls the behaviour of the
   * server
   * @param sparkSession a {@link SparkSession} for resolving Spark queries
   * @param database a {@link Database} for reading and writing resources
   * @param terminologyServiceFactory a {@link TerminologyServiceFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders a {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param aggregateExecutor a {@link AggregateExecutor} for processing requests to the aggregate
   * operation
   * @param extractExecutor a {@link ExtractExecutor} for processing requests to the extract
   */
  public ResourceProviderFactory(
      @Nonnull final ApplicationContext applicationContext,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final Database database,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final AggregateExecutor aggregateExecutor,
      @Nonnull final ExtractExecutor extractExecutor) {
    this.applicationContext = applicationContext;
    this.fhirContext = fhirContext;
    this.configuration = configuration;
    this.sparkSession = sparkSession;
    this.database = database;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.fhirEncoders = fhirEncoders;
    this.aggregateExecutor = aggregateExecutor;
    this.extractExecutor = extractExecutor;
  }

  /**
   * Creates a {@link AggregateProvider} bean for given resource type.
   *
   * @param resourceType the type of resource to create the provider for.
   * @return {@link AggregateProvider} bean.
   */
  @Nonnull
  public IResourceProvider createAggregateResourceProvider(
      @Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(resourceType.name()).getImplementingClass();
    return applicationContext
        .getBean(AggregateProvider.class, aggregateExecutor, resourceTypeClass);
  }

  /**
   * Creates an {@link au.csiro.pathling.extract.ExtractProvider} bean for given resource type.
   *
   * @param resourceType the type of resource to create the provider for.
   * @return {@link au.csiro.pathling.extract.ExtractProvider} bean.
   */
  @Nonnull
  public IResourceProvider createExtractResourceProvider(
      @Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(resourceType.name()).getImplementingClass();
    return applicationContext
        .getBean(ExtractProvider.class, extractExecutor, resourceTypeClass);
  }

  /**
   * Creates a {@link SearchProvider} bean for given resource type.
   *
   * @param resourceType the type of resource to create the provider for.
   * @return the SearchProvider bean.
   */
  @Nonnull
  public SearchProvider createSearchResourceProvider(@Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(resourceType.name()).getImplementingClass();

    return applicationContext.getBean(SearchProvider.class, configuration, fhirContext,
        sparkSession, database, terminologyServiceFactory, fhirEncoders, resourceTypeClass);
  }

  @Nonnull
  public UpdateProvider createUpdateResourceProvider(@Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(resourceType.name()).getImplementingClass();

    return applicationContext.getBean(UpdateProvider.class, database, resourceTypeClass);
  }
}
