package au.csiro.pathling.fhir;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.aggregate.CachingAggregateExecutor;
import au.csiro.pathling.aggregate.FreshAggregateExecutor;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.search.CachingSearchProvider;
import au.csiro.pathling.search.SearchExecutorCache;
import au.csiro.pathling.search.SearchProvider;
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
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final SearchExecutorCache searchExecutorCache;

  public ResourceProviderFactory(
      @Nonnull final ApplicationContext applicationContext,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final Configuration configuration,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final SearchExecutorCache searchExecutorCache,
      @Nonnull final CachingAggregateExecutor cachingAggregateExecutor,
      @Nonnull final FreshAggregateExecutor freshAggregateExecutor
  ) {
    this.applicationContext = applicationContext;
    this.fhirContext = fhirContext;
    this.configuration = configuration;
    this.sparkSession = sparkSession;
    this.resourceReader = resourceReader;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.fhirEncoders = fhirEncoders;
    this.searchExecutorCache = searchExecutorCache;
    this.aggregateExecutor = configuration.getCaching().isEnabled()
                             ? cachingAggregateExecutor
                             : freshAggregateExecutor;
  }

  @Nonnull
  public IResourceProvider createAggregateResourceProvider(
      @Nonnull final ResourceType resourceType) {
    final Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(resourceType.name()).getImplementingClass();
    return applicationContext
        .getBean(AggregateProvider.class, aggregateExecutor, resourceTypeClass);
  }

  @Nonnull
  public IResourceProvider createSearchResourceProvider(@Nonnull final ResourceType resourceType, boolean cached) {
    final Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(resourceType.name()).getImplementingClass();

    final IResourceProvider searchProvider =
        cached
        ? applicationContext
            .getBean(CachingSearchProvider.class, configuration, fhirContext, sparkSession,
                resourceReader,
                terminologyServiceFactory, fhirEncoders, resourceTypeClass, searchExecutorCache)
        : applicationContext
            .getBean(SearchProvider.class, configuration, fhirContext, sparkSession, resourceReader,
                terminologyServiceFactory, fhirEncoders, resourceTypeClass);
    return searchProvider;
  }

}
