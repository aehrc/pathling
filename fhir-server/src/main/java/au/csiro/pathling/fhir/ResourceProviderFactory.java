package au.csiro.pathling.fhir;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.aggregate.AggregateExecutor;
import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.aggregate.CachingAggregateExecutor;
import au.csiro.pathling.aggregate.FreshAggregateExecutor;
import ca.uhn.fhir.rest.server.IResourceProvider;
import javax.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("server")
public class ResourceProviderFactory {

  @Nonnull
  private final ApplicationContext applicationContext;

  @Nonnull
  private final AggregateExecutor aggregateExecutor;


  public ResourceProviderFactory(@Nonnull ApplicationContext applicationContext,
      @Nonnull final Configuration configuration,
      @Nonnull final CachingAggregateExecutor cachingAggregateExecutor,
      @Nonnull final FreshAggregateExecutor freshAggregateExecutor) {
    this.applicationContext = applicationContext;
    this.aggregateExecutor = configuration.getCaching().isEnabled()
                             ? cachingAggregateExecutor
                             : freshAggregateExecutor;
  }

  @Nonnull
  public IResourceProvider createAggregateResourceProvider(
      @Nonnull final Class<? extends IBaseResource> resourceTypeClass) {
    return applicationContext
        .getBean(AggregateProvider.class, aggregateExecutor, resourceTypeClass);
  }

}
