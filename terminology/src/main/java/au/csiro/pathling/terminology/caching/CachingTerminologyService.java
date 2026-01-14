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

package au.csiro.pathling.terminology.caching;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.BaseTerminologyService;
import au.csiro.pathling.terminology.PropertyOrDesignationList;
import au.csiro.pathling.terminology.TerminologyOperation;
import au.csiro.pathling.terminology.TerminologyParameters;
import au.csiro.pathling.terminology.TerminologyResult;
import au.csiro.pathling.terminology.TranslationList;
import au.csiro.pathling.terminology.lookup.LookupExecutor;
import au.csiro.pathling.terminology.lookup.LookupParameters;
import au.csiro.pathling.terminology.subsumes.SubsumesExecutor;
import au.csiro.pathling.terminology.subsumes.SubsumesParameters;
import au.csiro.pathling.terminology.translate.TranslateExecutor;
import au.csiro.pathling.terminology.translate.TranslateParameters;
import au.csiro.pathling.terminology.validatecode.ValidateCodeExecutor;
import au.csiro.pathling.terminology.validatecode.ValidateCodeParameters;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A terminology service that uses embedded Infinispan to cache the results of the underlying
 * terminology service operations.
 *
 * @author John Grimes
 */
public abstract class CachingTerminologyService extends BaseTerminologyService {

  private static final String VALIDATE_CODE_CACHE_NAME = "validate-code";
  private static final String SUBSUMES_CACHE_NAME = "subsumes";
  private static final String TRANSLATE_CACHE_NAME = "translate";
  private static final String LOOKUP_CACHE_NAME = "lookup";
  private static final String ETAG_HEADER_NAME = "etag";
  private static final String IF_NONE_MATCH_HEADER_NAME = "if-none-match";
  private static final String CACHE_CONTROL_HEADER_NAME = "cache-control";

  /** The caching configuration for the HTTP client. */
  @Nonnull protected final HttpClientCachingConfiguration configuration;

  /** The embedded cache manager that manages all terminology caches. */
  @Nonnull protected final EmbeddedCacheManager cacheManager;

  /** Cache for storing code validation results. */
  @Nonnull protected final Cache<Integer, TerminologyResult<Boolean>> validateCodeCache;

  /** Cache for storing concept subsumption results. */
  @Nonnull
  protected final Cache<Integer, TerminologyResult<ConceptSubsumptionOutcome>> subsumesCache;

  /** Cache for storing translation results. */
  @Nonnull protected final Cache<Integer, TerminologyResult<TranslationList>> translateCache;

  /** Cache for storing lookup results including properties and designations. */
  @Nonnull protected final Cache<Integer, TerminologyResult<PropertyOrDesignationList>> lookupCache;

  /**
   * Creates a new caching terminology service.
   *
   * @param terminologyClient The terminology client to cache results from
   * @param configuration The caching configuration for the HTTP client
   * @param resourcesToClose Any resources that should be closed when this service is closed
   */
  @SuppressWarnings("unchecked")
  protected CachingTerminologyService(
      @Nonnull final TerminologyClient terminologyClient,
      @Nonnull final HttpClientCachingConfiguration configuration,
      @Nonnull final Closeable... resourcesToClose) {
    super(terminologyClient, resourcesToClose);
    this.configuration = configuration;
    // register manager as a closeable resource
    cacheManager = registerResource(buildCacheManager());
    validateCodeCache = buildCache(cacheManager, VALIDATE_CODE_CACHE_NAME, Boolean.class);
    subsumesCache = buildCache(cacheManager, SUBSUMES_CACHE_NAME, ConceptSubsumptionOutcome.class);
    translateCache = buildCache(cacheManager, TRANSLATE_CACHE_NAME, TranslationList.class);
    lookupCache = buildCache(cacheManager, LOOKUP_CACHE_NAME, PropertyOrDesignationList.class);
  }

  @Override
  public boolean validateCode(@Nonnull final String valueSetUrl, @Nonnull final Coding coding) {
    final ValidateCodeParameters parameters =
        new ValidateCodeParameters(valueSetUrl, ImmutableCoding.of(coding));
    final ValidateCodeExecutor executor = new ValidateCodeExecutor(terminologyClient, parameters);
    return getFromCache(validateCodeCache, parameters, executor);
  }

  @Nonnull
  @Override
  public TranslationList translate(
      @Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse,
      @Nullable final String target) {
    final TranslateParameters parameters =
        new TranslateParameters(ImmutableCoding.of(coding), conceptMapUrl, reverse, target);
    final TranslateExecutor executor = new TranslateExecutor(terminologyClient, parameters);
    return getFromCache(translateCache, parameters, executor);
  }

  @Nonnull
  @Override
  public ConceptSubsumptionOutcome subsumes(
      @Nonnull final Coding codingA, @Nonnull final Coding codingB) {
    final SubsumesParameters parameters =
        new SubsumesParameters(ImmutableCoding.of(codingA), ImmutableCoding.of(codingB));
    final SubsumesExecutor executor = new SubsumesExecutor(terminologyClient, parameters);
    return getFromCache(subsumesCache, parameters, executor);
  }

  @Nonnull
  @Override
  public PropertyOrDesignationList lookup(
      @Nonnull final Coding coding,
      @Nullable final String property,
      @Nullable final String acceptLanguage) {
    final LookupParameters parameters =
        new LookupParameters(ImmutableCoding.of(coding), property, acceptLanguage);
    final LookupExecutor executor = new LookupExecutor(terminologyClient, parameters);
    return getFromCache(lookupCache, parameters, executor);
  }

  /**
   * Gets the result of an operation from the cache, or fetches a new result if the cache is empty
   * or expired.
   *
   * @param cache The cache being used for this operation
   * @param parameters The parameters for the operation
   * @param operation A {@link TerminologyOperation} that provides the behavior specific to the type
   *     of operation
   * @param <P> The type of the parameters that are input to the operation
   * @param <S> The type of the response returned by the terminology client
   * @param <R> The type of the final result that is extracted from the response
   * @return The operation result
   */
  private <P extends TerminologyParameters, S, R extends Serializable> R getFromCache(
      @Nonnull final Cache<Integer, TerminologyResult<R>> cache,
      @Nonnull final P parameters,
      @Nonnull final TerminologyOperation<S, R> operation) {
    final int key = parameters.hashCode();
    final TerminologyResult<R> cached = cache.get(key);

    if (cached == null) {
      // Cache miss.
      final TerminologyResult<R> result = fetch(operation, Optional.empty());
      cache.put(key, result);
      return requireNonNull(result.getData());
    } else {
      final boolean expired =
          cached.getExpires() != null && System.currentTimeMillis() > cached.getExpires();
      final TerminologyResult<R> result =
          cache.compute(
              key,
              (k, v) ->
                  expired
                      // Cache hit, but the entry is expired and needs to be revalidated.
                      ? fetch(operation, Optional.ofNullable(v))
                      // Cache hit, and the entry is still valid.
                      : v);
      requireNonNull(result);
      return requireNonNull(result.getData());
    }
  }

  /**
   * Fetches an operation result, or revalidates it if the cached result is still valid.
   *
   * @param operation A {@link TerminologyOperation} that provides the behavior specific to the type
   *     of operation
   * @param cached A previously cached value
   * @param <S> The type of the response returned by the terminology client
   * @param <R> The type of the final result that is extracted from the response
   * @return The operation result
   */
  private <S, R extends Serializable> TerminologyResult<R> fetch(
      @Nonnull final TerminologyOperation<S, R> operation,
      @Nonnull final Optional<TerminologyResult<R>> cached) {
    final Optional<R> invalidResult = operation.validate();
    if (invalidResult.isPresent()) {
      // If the parameters fail validation, cache the invalid result forever.
      return new TerminologyResult<>(invalidResult.get(), null, null, false);
    }

    final IOperationUntypedWithInput<S> request = operation.buildRequest();

    // Add an If-None-Match header if the cached result is accompanied by an ETag.
    cached
        .flatMap(c -> Optional.ofNullable(c.getEtag()))
        .ifPresent(tag -> request.withAdditionalHeader(IF_NONE_MATCH_HEADER_NAME, tag));

    // We use a default expiry if the server does not provide one.
    final Optional<Long> defaultExpires =
        Optional.of(secondsFromNow(configuration.getDefaultExpiry()));
    // There may be a configured override expiry.
    final Optional<Long> overrideExpires =
        Optional.ofNullable(configuration.getOverrideExpiry())
            .map(CachingTerminologyService::secondsFromNow);

    try {
      final MethodOutcome outcome = request.returnMethodOutcome().execute();

      // If the response was 200 OK, use the data from the fresh response.
      @SuppressWarnings("unchecked")
      final R result = operation.extractResult((S) outcome.getResource());
      final Optional<String> newEtag = getSingularHeader(outcome, ETAG_HEADER_NAME);
      final Optional<Long> serverExpires = getExpires(outcome);
      return new TerminologyResult<>(
          result,
          newEtag.orElse(null),
          // Expiry values are used in this order:
          // 1. The override expiry, if present;
          // 2. The expiry provided by the server, if present, then;
          // 3. The default expiry.
          resolveExpires(List.of(overrideExpires, serverExpires, defaultExpires)),
          false);

    } catch (final NotModifiedException e) {
      // If the response was 304 Not Modified, use the data from the cached response and update
      // the ETag and expiry.
      final Optional<String> newEtag = getSingularHeader(e.getResponseHeaders(), ETAG_HEADER_NAME);
      final TerminologyResult<R> previous = checkPresent(cached);
      final Optional<Long> serverExpires = getExpires(e.getResponseHeaders());
      final Optional<Long> previousExpiry = Optional.ofNullable(previous.getExpires());
      return new TerminologyResult<>(
          previous.getData(),
          newEtag.orElse(previous.getEtag()),
          // Expiry values are used in this order:
          // 1. The override expiry, if present;
          // 2. The expiry from the 304 response, if present;
          // 3. The expiry from the cached response, if present, then;
          // 4. The default expiry.
          resolveExpires(List.of(overrideExpires, serverExpires, previousExpiry, defaultExpires)),
          false);

    } catch (final BaseServerResponseException e) {
      // If the terminology server rejects the request as invalid, cache the invalid result for the
      // amount of time instructed by the server. If there is no such instruction, cache it for the
      // configured default expiry.
      final Optional<Long> serverExpires = getExpires(e.getResponseHeaders());
      final long expires = resolveExpires(List.of(serverExpires, defaultExpires));
      final TerminologyResult<R> fallback =
          new TerminologyResult<>(operation.invalidRequestFallback(), null, expires, false);
      return handleError(e, fallback);
    }
  }

  /**
   * Builds a cache manager for this terminology service.
   *
   * @return a new {@link EmbeddedCacheManager} instance appropriate for the specific implementation
   */
  protected abstract EmbeddedCacheManager buildCacheManager();

  /**
   * Builds a cache for storing terminology results.
   *
   * @param <T> the type of the cache value
   * @param cacheManager the {@link EmbeddedCacheManager} to use to construct the cache
   * @param cacheName a name for the cache
   * @param valueType the class of the cache value type
   * @return a new {@link Cache} instance appropriate for the specific implementation
   */
  protected abstract <T extends Serializable> Cache<Integer, TerminologyResult<T>> buildCache(
      @Nonnull final EmbeddedCacheManager cacheManager,
      @Nonnull final String cacheName,
      @Nonnull final Class<T> valueType);

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  private static Optional<String> getSingularHeader(
      @Nonnull final MethodOutcome outcome, @Nonnull final String headerName) {
    return getSingularHeader(outcome.getResponseHeaders(), headerName);
  }

  @Nonnull
  private static Optional<String> getSingularHeader(
      @Nullable final Map<String, List<String>> headers, @Nonnull final String headerName) {
    return Optional.ofNullable(headers)
        .map(rh -> rh.get(headerName))
        .flatMap(h -> h.stream().findFirst());
  }

  @Nonnull
  private static Optional<Long> getExpires(@Nonnull final MethodOutcome outcome) {
    return getExpires(outcome.getResponseHeaders());
  }

  @Nonnull
  private static Optional<Long> getExpires(@Nullable final Map<String, List<String>> headers) {
    return getSingularHeader(headers, CACHE_CONTROL_HEADER_NAME)
        .flatMap(CachingTerminologyService::parseMaxAgeFromCacheControl)
        .map(CachingTerminologyService::secondsFromNow);
  }

  @Nonnull
  private static Optional<Integer> parseMaxAgeFromCacheControl(@Nonnull final String cacheControl) {
    final String[] parts = cacheControl.split(",\\s*");
    for (final String part : parts) {
      if (part.startsWith("max-age")) {
        final String argument = part.split("=")[1];
        try {
          return Optional.of(Integer.parseInt(argument));
        } catch (final NumberFormatException e) {
          return Optional.empty();
        }
      }
    }
    return Optional.empty();
  }

  private static long secondsFromNow(final int seconds) {
    return Instant.now().plusSeconds(seconds).toEpochMilli();
  }

  private static Long resolveExpires(@Nonnull final List<Optional<Long>> orderedExpiryValues) {
    // Go through each of the expiry values and combine them using OR logic. If the final value is
    // not present, throw an error.
    return orderedExpiryValues.stream()
        .reduce((acc, v) -> acc.or(() -> v))
        .orElseThrow()
        .orElseThrow();
  }
}
