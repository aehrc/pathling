/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.search;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Implements a layer of caching around the resource retrieval code within the SearchExecutor. This
 * effectively caches individual requests within a particular search (i.e. each page of results),
 * but does not cache the search itself. Caching of a search based upon its parameters is performed
 * by {@link SearchExecutorCache}.
 * <p>
 * This class does not implement {@link au.csiro.pathling.caching.Cacheable}, as it is desirable to
 * keep the integrity of a search's results intact regardless of cache invalidation. Caching of the
 * search itself is invalidated through SearchExecutorCache and {@link CachingSearchProvider}.
 *
 * @author John Grimes
 */
public class CachingSearchExecutor implements IBundleProvider {

  @Nonnull
  private final SearchExecutor delegate;

  @Nonnull
  private final LoadingCache<SearchCacheKey, List<IBaseResource>> cache;

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   * queries within parallel processing
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * objects
   * @param subjectResource The type of resource that is the subject for this query
   * @param filters A list of filters that should be applied within queries
   */
  public CachingSearchExecutor(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<StringAndListParam> filters) {
    delegate = new SearchExecutor(configuration, fhirContext, sparkSession, resourceReader,
        terminologyServiceFactory, fhirEncoders, subjectResource, filters);
    cache = initializeCache(configuration.getCaching().getSearchPageCacheSize());
  }

  private LoadingCache<SearchCacheKey, List<IBaseResource>> initializeCache(
      final long maximumSize) {
    return CacheBuilder.newBuilder()
        .maximumSize(maximumSize)
        .build(new CacheLoader<>() {
          @Override
          public List<IBaseResource> load(@Nonnull final SearchCacheKey key) {
            return delegate.getResources(key.getFromIndex(), key.getToIndex());
          }
        });
  }

  @Override
  public IPrimitiveType<Date> getPublished() {
    return delegate.getPublished();
  }

  @Nonnull
  @Override
  public List<IBaseResource> getResources(final int theFromIndex, final int theToIndex) {
    return cache.getUnchecked(new SearchCacheKey(theFromIndex, theToIndex));
  }

  @Nullable
  @Override
  public String getUuid() {
    return delegate.getUuid();
  }

  @Nullable
  @Override
  public Integer preferredPageSize() {
    return delegate.preferredPageSize();
  }

  @Nullable
  @Override
  public Integer size() {
    return delegate.size();
  }

  @Getter
  private static class SearchCacheKey {

    private final int fromIndex;
    private final int toIndex;

    private SearchCacheKey(final int fromIndex, final int toIndex) {
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SearchCacheKey that = (SearchCacheKey) o;
      return fromIndex == that.fromIndex &&
          toIndex == that.toIndex;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fromIndex, toIndex);
    }

  }

}
