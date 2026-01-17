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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
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

  @Test
  void jobEndpointSkipsEtagValidation() {
    // $job requests should not use ETag-based revalidation. They use TTL-based caching instead.
    setupCacheableRequest("GET", TAG, "$job");

    // Should NOT throw NotModifiedException - let JobProvider handle caching.
    interceptor.checkIncomingTag(request, requestDetails, response);

    // Should not check database cache key for $job.
    verify(database, org.mockito.Mockito.never())
        .cacheKeyMatches(org.mockito.ArgumentMatchers.anyString());
    verifyCacheableResponseHeaders();
    verify(response).setHeader(eq("Cache-Control"), eq("must-revalidate,max-age=1"));
  }

  @Test
  void resultEndpointSkipsEtagValidation() {
    // $result requests should not use ETag-based revalidation. They use TTL-based caching instead.
    setupCacheableRequest("GET", TAG, "$result");

    interceptor.checkIncomingTag(request, requestDetails, response);

    verify(database, org.mockito.Mockito.never())
        .cacheKeyMatches(org.mockito.ArgumentMatchers.anyString());
    verifyCacheableResponseHeaders();
    verify(response).setHeader(eq("Cache-Control"), eq("must-revalidate,max-age=1"));
  }
}
