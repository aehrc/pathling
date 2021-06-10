/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.caching.Cacheable;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class wraps another {@link AggregateExecutor}, caching requests in memory.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
@Slf4j
public class CachingAggregateExecutor implements AggregateExecutor, Cacheable {

  @Nonnull
  private final AggregateExecutor delegate;

  @Nonnull
  private final LoadingCache<AggregateRequest, AggregateResponse> cache;

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyClientFactory A {@link TerminologyServiceFactory} for resolving terminology
   */
  public CachingAggregateExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyClientFactory) {
    delegate = new FreshAggregateExecutor(configuration, fhirContext, sparkSession, resourceReader,
        terminologyClientFactory);
    cache = initializeCache(configuration.getCaching().getAggregateRequestCacheSize());
  }

  private LoadingCache<AggregateRequest, AggregateResponse> initializeCache(
      final long maximumSize) {
    return CacheBuilder.newBuilder()
        .maximumSize(maximumSize)
        .build(
            new CacheLoader<>() {
              @Override
              public AggregateResponse load(@Nonnull final AggregateRequest request) {
                return delegate.execute(request);
              }
            }
        );
  }

  @Override
  public AggregateResponse execute(@Nonnull final AggregateRequest query) {
    log.info("Received request: {}", query);
    // We use `getUnchecked` here to avoid wrapping HAPI exceptions with a checked
    // ExecutionException.
    return cache.getUnchecked(query);
  }

  @Override
  public void invalidateCache() {
    cache.invalidateAll();
  }

}
