/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.extract.ExtractExecutor;
import au.csiro.pathling.extract.ExtractProvider;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.search.SearchProvider;
import au.csiro.pathling.update.UpdateHelpers;
import au.csiro.pathling.update.UpdateProvider;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.IResourceProvider;
import java.util.Optional;
import javax.annotation.Nonnull;
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
  private final Configuration configuration;

  @Nonnull
  private final AggregateExecutor aggregateExecutor;

  @Nonnull
  private final ExtractExecutor extractExecutor;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final ResourceWriter resourceWriter;

  @Nonnull
  private final UpdateHelpers updateHelpers;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final CacheInvalidator cacheInvalidator;

  /**
   * @param applicationContext The Spring {@link ApplicationContext}
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param configuration A {@link Configuration} instance which controls the behaviour of the
   * server
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param aggregateExecutor A {@link AggregateExecutor} for processing requests to the aggregate
   * operation
   * @param extractExecutor A {@link ExtractExecutor} for processing requests to the extract
   */
  public ResourceProviderFactory(
      @Nonnull final ApplicationContext applicationContext,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final Configuration configuration,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final AggregateExecutor aggregateExecutor,
      @Nonnull final ExtractExecutor extractExecutor,
      @Nonnull final ResourceWriter resourceWriter,
      @Nonnull final UpdateHelpers updateHelpers,
      @Nonnull final CacheInvalidator cacheInvalidator) {
    this.applicationContext = applicationContext;
    this.fhirContext = fhirContext;
    this.configuration = configuration;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.fhirEncoders = fhirEncoders;
    this.aggregateExecutor = aggregateExecutor;
    this.extractExecutor = extractExecutor;
    this.resourceWriter = resourceWriter;
    this.updateHelpers = updateHelpers;
    this.cacheInvalidator = cacheInvalidator;
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
        sparkSession, resourceReader, terminologyServiceFactory, fhirEncoders, resourceTypeClass);
  }

  @Nonnull
  public UpdateProvider createUpdateResourceProvider(@Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(resourceType.name()).getImplementingClass();

    return applicationContext.getBean(UpdateProvider.class, sparkSession, fhirEncoders,
            resourceReader, resourceWriter, updateHelpers, cacheInvalidator, resourceTypeClass);
  }
}
