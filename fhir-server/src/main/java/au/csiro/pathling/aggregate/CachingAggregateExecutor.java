/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.caching.Cacheable;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * This class wraps another {@link AggregateExecutor}, caching requests in memory.
 *
 * @author John Grimes
 */
@Component
@Primary
@ConditionalOnProperty(prefix = "pathling", value = "caching.enabled", havingValue = "true")
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
   * @param terminologyClient A {@link TerminologyClient} for resolving terminology queries
   * @param terminologyClientFactory A {@link TerminologyClientFactory} for resolving terminology
   */
  public CachingAggregateExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory) {
    delegate = new FreshAggregateExecutor(configuration, fhirContext, sparkSession, resourceReader,
        terminologyClient, terminologyClientFactory);
    cache = initializeCache(configuration.getCaching().getMaxEntries());
  }

  private LoadingCache<AggregateRequest, AggregateResponse> initializeCache(
      final long maximumSize) {
    return CacheBuilder.newBuilder()
        .maximumSize(maximumSize)
        .build(
            new CacheLoader<AggregateRequest, AggregateResponse>() {
              @Override
              public AggregateResponse load(@Nonnull final AggregateRequest request) {
                return delegate.execute(request);
              }
            }
        );
  }

  @Override
  public AggregateResponse execute(@Nonnull final AggregateRequest query) {
    try {
      log.info("Received request: " + query);
      // We use `getUnchecked` here to avoid wrapping HAPI exceptions with a checked 
      // ExecutionException.
      return cache.getUnchecked(query);

    } catch (final UncheckedExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void invalidateCache() {
    cache.invalidateAll();
  }

}
