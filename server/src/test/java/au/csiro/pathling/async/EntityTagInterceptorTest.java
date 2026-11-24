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

package au.csiro.pathling.async;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.cache.EntityTagInterceptor;
import au.csiro.pathling.config.HttpServerCachingConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.fhir.ConformanceProvider;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class EntityTagInterceptorTest {

  static final String TAG = "abc123";
  static final String CURRENT_INSTANCE_ID = "abc12345";

  CacheableDatabase database;
  ConformanceProvider conformanceProvider;
  HttpServletRequest request;
  RequestDetails requestDetails;
  HttpServletResponse response;
  EntityTagInterceptor interceptor;
  ServerInstanceId serverInstanceId;

  @BeforeEach
  void setUp() {
    database = mock(CacheableDatabase.class);
    conformanceProvider = mock(ConformanceProvider.class);
    request = mock(HttpServletRequest.class);
    requestDetails = mock(RequestDetails.class);
    response = mock(HttpServletResponse.class);
    serverInstanceId = mock(ServerInstanceId.class);
    when(serverInstanceId.getId()).thenReturn(CURRENT_INSTANCE_ID);
    final ServerConfiguration configuration = mock(ServerConfiguration.class);
    final HttpServerCachingConfiguration httpCaching = mock(HttpServerCachingConfiguration.class);
    when(httpCaching.getVary())
        .thenReturn(List.of("Accept", "Accept-Encoding", "Prefer", "Authorization"));
    when(httpCaching.getCacheableControl()).thenReturn(List.of("must-revalidate", "max-age=1"));
    when(configuration.getHttpCaching()).thenReturn(httpCaching);
    interceptor =
        new EntityTagInterceptor(configuration, database, conformanceProvider, serverInstanceId);
  }

  @Test
  void setsETagAndCacheControl() {
    setupCacheableRequest("GET", null, "$aggregate");
    when(database.cacheKeyMatches(isNull())).thenReturn(false);
    when(database.getCacheKey()).thenReturn(Optional.of(TAG));

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void returnsNotModified() {
    setupCacheableRequest("GET", TAG, "$aggregate");
    when(database.cacheKeyMatches(eq(TAG))).thenReturn(true);

    assertThrows(
        NotModifiedException.class,
        () -> interceptor.checkIncomingTag(request, requestDetails, response));

    verifyCacheableResponseHeaders();
  }

  @Test
  void setsETagAndCacheControlForConformance() {
    setupCacheableRequest("GET", null, "metadata");
    when(conformanceProvider.cacheKeyMatches(isNull())).thenReturn(false);
    when(conformanceProvider.getCacheKey()).thenReturn(Optional.of(TAG));

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void returnsNotModifiedForConformance() {
    setupCacheableRequest("GET", TAG, "metadata");
    when(conformanceProvider.cacheKeyMatches(eq(TAG))).thenReturn(true);

    assertThrows(
        NotModifiedException.class,
        () -> interceptor.checkIncomingTag(request, requestDetails, response));

    verifyCacheableResponseHeaders();
  }

  @Test
  void setsETagForExtractRequest() {
    setupCacheableRequest("GET", null, "$extract");
    when(database.cacheKeyMatches(isNull())).thenReturn(false);
    when(database.getCacheKey()).thenReturn(Optional.of(TAG));

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void setsETagForSearchRequest() {
    setupCacheableRequest("GET", null, null);
    when(database.cacheKeyMatches(isNull())).thenReturn(false);
    when(database.getCacheKey()).thenReturn(Optional.of(TAG));

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void setsETagForHead() {
    setupCacheableRequest("HEAD", null, "$aggregate");
    when(database.cacheKeyMatches(isNull())).thenReturn(false);
    when(database.getCacheKey()).thenReturn(Optional.of(TAG));

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @Test
  void doesNothingWhenNotCacheable() {
    setupCacheableRequest("POST", null, "$aggregate");

    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyNoInteractions(database);
    verifyNoInteractions(response);
  }

  void setupCacheableRequest(
      @Nonnull final String method, @Nullable final String tag, @Nullable final String operation) {
    when(request.getMethod()).thenReturn(method);
    when(request.getHeader(eq("If-None-Match"))).thenReturn("W/\"" + tag + "\"");
    when(requestDetails.getOperation()).thenReturn(operation);
  }

  void verifyMissResponseHeaders() {
    verifyCacheableResponseHeaders();
    verify(response).setHeader(eq("ETag"), eq("W/\"" + TAG + "\""));
    verify(response).setHeader(eq("Cache-Control"), eq("must-revalidate,max-age=1"));
  }

  void verifyCacheableResponseHeaders() {
    verify(response).addHeader(eq("Vary"), eq("Accept,Accept-Encoding,Prefer,Authorization"));
  }

  @Test
  void asyncEtagWithMismatchedInstanceSkips304() {
    // When an async ETag from a different server instance is received, the interceptor should
    // NOT return 304, allowing the request to proceed and create a new job.
    final String differentInstanceId = "diffrent1";
    final String jobIdHash = "f8a9b2c3";
    final String asyncTag = "~" + differentInstanceId + "." + jobIdHash;
    setupCacheableRequest("GET", asyncTag, "$export");
    when(database.cacheKeyMatches(eq(asyncTag))).thenReturn(false);
    when(database.getCacheKey()).thenReturn(Optional.of(TAG));

    // Should NOT throw NotModifiedException, allowing request to proceed.
    interceptor.checkIncomingTag(request, requestDetails, response);

    // Should set Vary and Cache-Control, but NOT ETag (the async aspect will set that later).
    verifyCacheableResponseHeaders();
    verify(response).setHeader(eq("Cache-Control"), eq("must-revalidate,max-age=1"));
  }

  @Test
  void asyncEtagWithMatchingInstanceReturns304() {
    // When an async ETag from the SAME server instance is received, normal caching applies.
    final String jobIdHash = "f8a9b2c3";
    final String asyncTag = "~" + CURRENT_INSTANCE_ID + "." + jobIdHash;
    setupCacheableRequest("GET", asyncTag, "$export");

    // For matching instance, we check against the async tag registry.
    when(database.cacheKeyMatches(eq(asyncTag))).thenReturn(true);

    assertThrows(
        NotModifiedException.class,
        () -> interceptor.checkIncomingTag(request, requestDetails, response));

    verifyCacheableResponseHeaders();
  }

  @Test
  void malformedAsyncEtagWithoutDotSeparatorTreatedAsNormal() {
    // An async ETag without a dot separator should be treated as a normal tag, not as an async
    // ETag. This handles cases where the format is malformed.
    final String malformedAsyncTag = "~abc12345f8a9b2c3"; // Missing dot separator.
    setupCacheableRequest("GET", malformedAsyncTag, "$export");
    when(database.cacheKeyMatches(eq(malformedAsyncTag))).thenReturn(false);
    when(database.getCacheKey()).thenReturn(Optional.of(TAG));

    // Should proceed normally without special async ETag handling.
    interceptor.checkIncomingTag(request, requestDetails, response);

    verifyMissResponseHeaders();
  }

  @ParameterizedTest
  @ValueSource(strings = {"$job", "$job-result", "$jobs", "$result"})
  void asyncEndpointSkipsEtagValidation(@Nonnull final String operation) {
    // Async endpoints report on the state of a particular job (or the job registry), not on the
    // state of the database, so they use TTL-based caching instead of the database ETag. Even when
    // the supplied tag matches the database, the request must not be answered with a 304, and the
    // response must not carry the database ETag, which would be shared across every job
    // (aehrc/pathling#2529).
    setupCacheableRequest("GET", TAG, operation);
    when(database.cacheKeyMatches(eq(TAG))).thenReturn(true);
    when(database.getCacheKey()).thenReturn(Optional.of(TAG));

    interceptor.checkIncomingTag(request, requestDetails, response);

    verify(database, never()).cacheKeyMatches(anyString());
    verify(database, never()).getCacheKey();
    verify(response, never()).setHeader(eq("ETag"), anyString());
    verifyCacheableResponseHeaders();
    verify(response).setHeader(eq("Cache-Control"), eq("must-revalidate,max-age=1"));
  }
}
