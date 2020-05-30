/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.search;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.caching.Cacheable;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.stereotype.Component;

/**
 * This class implements a cache of {@link IBundleProvider} instances returned as part of search
 * queries.
 *
 * @author John Grimes
 */
@Component
public class SearchExecutorCache implements Cacheable {

  @Nonnull
  private final LoadingCache<SearchExecutorCacheKey, IBundleProvider> cache;

  /**
   * @param configuration A {@link Configuration} to control the behaviour of the cache
   */
  public SearchExecutorCache(@Nonnull final Configuration configuration) {
    cache = CacheBuilder.newBuilder()
        .maximumSize(configuration.getCaching().getMaxEntries())
        .build(new SearchExecutorCacheLoader());
  }

  @Nonnull
  public IBundleProvider get(@Nonnull final SearchExecutorCacheKey key) {
    return cache.getUnchecked(key);
  }

  @Override
  public void invalidateCache() {
    cache.invalidateAll();
  }

  private static class SearchExecutorCacheLoader extends
      CacheLoader<SearchExecutorCacheKey, IBundleProvider> {

    @Nonnull
    @Override
    public IBundleProvider load(@Nonnull final SearchExecutorCacheKey key) {
      return new CachingSearchExecutor(key.getConfiguration(), key.getFhirContext(),
          key.getSparkSession(), key.getResourceReader(), key.getTerminologyClient(),
          key.getTerminologyClientFactory(), key.getFhirEncoders(), key.getSubjectResource(),
          key.getFilters());
    }

  }

  /**
   * The key required to retrieve a value from the cache. {@code subjectResource} and {@code
   * filters} are the two values which control the uniqueness of the key within the cache.
   */
  @Getter
  public static class SearchExecutorCacheKey {

    @Nonnull
    private final Configuration configuration;

    @Nonnull
    private final FhirContext fhirContext;

    @Nonnull
    private final SparkSession sparkSession;

    @Nonnull
    private final ResourceReader resourceReader;

    @Nonnull
    private final Optional<TerminologyClient> terminologyClient;

    @Nonnull
    private final Optional<TerminologyClientFactory> terminologyClientFactory;

    @Nonnull
    private final FhirEncoders fhirEncoders;

    @Nonnull
    private final ResourceType subjectResource;

    @Nonnull
    private final Optional<StringAndListParam> filters;

    /**
     * @param configuration A {@link Configuration} object to control the behaviour of the executor
     * @param fhirContext A {@link FhirContext} for doing FHIR stuff
     * @param sparkSession A {@link SparkSession} for resolving Spark queries
     * @param resourceReader A {@link ResourceReader} for retrieving resources
     * @param terminologyClient A {@link TerminologyClient} for resolving terminology queries
     * @param terminologyClientFactory A {@link TerminologyClientFactory} for resolving terminology
     * queries within parallel processing
     * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
     * objects
     * @param subjectResource The type of resource that is the subject for this query
     * @param filters A list of filters that should be applied within queries
     */
    public SearchExecutorCacheKey(@Nonnull final Configuration configuration,
        @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
        @Nonnull final ResourceReader resourceReader,
        @Nonnull final Optional<TerminologyClient> terminologyClient, @Nonnull final
    Optional<TerminologyClientFactory> terminologyClientFactory,
        @Nonnull final FhirEncoders fhirEncoders,
        @Nonnull final ResourceType subjectResource,
        @Nonnull final Optional<StringAndListParam> filters) {
      this.configuration = configuration;
      this.fhirContext = fhirContext;
      this.sparkSession = sparkSession;
      this.resourceReader = resourceReader;
      this.terminologyClient = terminologyClient;
      this.terminologyClientFactory = terminologyClientFactory;
      this.fhirEncoders = fhirEncoders;
      this.subjectResource = subjectResource;
      this.filters = filters;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SearchExecutorCacheKey that = (SearchExecutorCacheKey) o;
      return subjectResource == that.subjectResource &&
          ((!filters.isPresent() && !that.filters.isPresent())
              || (filters.toString().equals(that.filters.toString())));
    }

    @Override
    public int hashCode() {
      if (!filters.isPresent()) {
        return Objects.hashCode(Optional.empty());
      } else {
        return Objects.hash(subjectResource, filters.toString());
      }
    }

  }

}
