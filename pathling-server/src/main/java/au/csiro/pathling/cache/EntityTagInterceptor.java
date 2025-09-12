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

package au.csiro.pathling.cache;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.fhir.ConformanceProvider;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Intercepts requests and validates ETags, skipping processing if possible. Also adds ETags to
 * responses.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Interceptor
@Slf4j
public class EntityTagInterceptor {

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private final CacheableDatabase database;

  @Nonnull
  private final ConformanceProvider conformanceProvider;

  public static final String DEFAULT_ETAG = "0";
  private static final Pattern ETAG_HEADER_PATTERN = Pattern.compile("^W/\"([^\"]+)\"$");

  /**
   * @param configuration configuration controlling the behaviour of the interceptor
   * @param database {@link Database} for use in retrieving cache keys
   * @param conformanceProvider for determining the cacheability of conformance statement requests
   */
  public EntityTagInterceptor(@Nonnull final ServerConfiguration configuration,
      @Nonnull final CacheableDatabase database,
      @Nonnull final ConformanceProvider conformanceProvider) {
    this.configuration = configuration;
    this.database = database;
    this.conformanceProvider = conformanceProvider;
  }

  /**
   * Checks for the {@code If-None-Match} header and validates the tag, skipping processing if
   * possible. Also, adds an {@code ETag} header to the response.
   *
   * @param request the servlet request object
   * @param requestDetails the details about the request inferred by HAPI
   * @param response the servlet response object
   */
  @Hook(value = Pointcut.SERVER_INCOMING_REQUEST_POST_PROCESSED, order = 2)
  @SuppressWarnings("unused")
  public void checkIncomingTag(@Nullable final HttpServletRequest request,
      @Nullable final RequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {
    requireNonNull(request);
    requireNonNull(response);
    requireNonNull(requestDetails);
    if (requestIsCacheable(request)) {
      // Set the Vary header to instruct HTTP caches on how to construct the cache key.
      final String varyValues = String.join(",", configuration.getHttpCaching().getVary());
      response.addHeader("Vary", varyValues);

      // Check the incoming request for an If-None-Match header.
      final String tagHeader = parseEtagValue(request.getHeader("If-None-Match"));

      // If the request is for the conformance statement, we use the conformance provider to 
      // determine whether it is fresh or not. The freshness of all other requests is determined by 
      // the database.
      final boolean conformance =
          requestDetails.getOperation() != null && requestDetails.getOperation().equals("metadata");
      final Cacheable cacheable = conformance
                                  ? conformanceProvider
                                  : database;

      final boolean tagMatches = cacheable.cacheKeyMatches(tagHeader);
      if (tagMatches) {
        // If there is a matching condition, we can skip processing and return a 304 Not Modified.
        log.debug("Entity tag validation succeeded, processing not required");
        throw new NotModifiedException("Supplied entity tag matches");
      } else {
        // If there is no matching condition, we add an ETag to the response, along with headers 
        // indicating that the response is cacheable.
        final String cacheControlValues = String.join(",",
            configuration.getHttpCaching().getCacheableControl());
        final String etag = cacheable.getCacheKey()
            .map(EntityTagInterceptor::quoteEtagValue)
            .orElse(DEFAULT_ETAG);
        response.setHeader("ETag", etag);
        response.setHeader("Cache-Control", cacheControlValues);
      }
    }
  }

  /**
   * Sets caching headers on a response to make sure that it doesn't get cached.
   *
   * @param response a {@link HttpServletResponse} upon which to set caching headers
   * @param configuration the {@link ServerConfiguration} for the server, which controls which
   * header values are used
   */
  public static void makeRequestNonCacheable(@Nullable final HttpServletResponse response,
      @Nonnull final ServerConfiguration configuration) {
    if (response == null) {
      return;
    }
    // We set the ETag to this value because we can't unset it, and this value won't match any valid
    // tag.
    response.setHeader("ETag", quoteEtagValue(DEFAULT_ETAG));

    // The Cache-Control header is set to indicate that the response should not be cached.
    final String cacheControlValues = String.join(",",
        configuration.getHttpCaching().getUncacheableControl());
    response.addHeader("Cache-Control", cacheControlValues);
  }

  /**
   * Checks whether we regard this request as cacheable.
   *
   * @param request a {@link HttpServletRequest}
   * @return true if the request is cacheable, false otherwise
   */
  public static boolean requestIsCacheable(@Nonnull final HttpServletRequest request) {
    return (request.getMethod().equals("GET") || request.getMethod().equals("HEAD"));
  }

  @Nonnull
  private static String quoteEtagValue(@Nonnull final String tag) {
    return "W/\"" + tag + "\"";
  }

  @Nonnull
  private static String parseEtagValue(@Nullable final String headerValue) {
    if (headerValue == null) {
      return DEFAULT_ETAG;
    }
    final Matcher matcher = ETAG_HEADER_PATTERN.matcher(headerValue);
    if (!matcher.matches()) {
      return DEFAULT_ETAG;
    }
    final String match = matcher.group(1);
    return match == null
           ? DEFAULT_ETAG
           : match;
  }

}
