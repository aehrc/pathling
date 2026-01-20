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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.cache.Cacheable;
import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.AsyncConfiguration;
import au.csiro.pathling.config.HttpServerCachingConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;

class RequestTagFactoryTest {

  private final CacheableDatabase mockDatabase = mock(CacheableDatabase.class);
  private final Cacheable mockCacheable = mock(Cacheable.class);
  private final ServletRequestDetails mockRequestDetails = mock(ServletRequestDetails.class);
  private final Authentication mockAuthentication = mock(Authentication.class);

  @Test
  void testComputesSalientHeaderNamesCorrectly() {

    final List<String> varyHeaders = List.of("Accept", "Accept-Encoding", "Authorization");
    final List<String> whiteListedHeaders = List.of("Accept", "Accept-Encoding", "X-UserId");

    assertEquals(
        Collections.emptySet(),
        new RequestTagFactory(
                mockDatabase,
                createServerConfiguration(Collections.emptyList(), Collections.emptyList()))
            .getSalientHeaderNames());

    assertEquals(
        Collections.emptySet(),
        new RequestTagFactory(
                mockDatabase,
                createServerConfiguration(Collections.emptyList(), whiteListedHeaders))
            .getSalientHeaderNames());

    assertEquals(
        varyHeaders.stream().map(String::toLowerCase).collect(Collectors.toUnmodifiableSet()),
        new RequestTagFactory(
                mockDatabase, createServerConfiguration(varyHeaders, Collections.emptyList()))
            .getSalientHeaderNames());

    assertEquals(
        Set.of("authorization"),
        new RequestTagFactory(
                mockDatabase, createServerConfiguration(varyHeaders, whiteListedHeaders))
            .getSalientHeaderNames());
  }

  @Test
  void testComputesCorrectTagForAuthenticatedUserAndExisingCacheKey() {
    final Set<String> salientHeaderNames =
        Set.of("X-Single-Value", "X-Multi-Values", "X-Not-Present");
    // RequestDetails.gethHeaders() returns a Map<String, List<String>> where the keys are
    // lower-cased.
    final Map<String, List<String>> requestHeaders =
        Map.of(
            "x-single-value",
            List.of("singleValue"),
            "x-multi-values",
            List.of("multiValue1", "multiValue2"),
            "x-other",
            List.of("otherValue"));
    final Object principal = new Object();
    when(mockAuthentication.getPrincipal()).thenReturn(principal);

    when(mockCacheable.getCacheKey()).thenReturn(Optional.of("cacheKey_A"));
    when(mockRequestDetails.getCompleteUrl()).thenReturn("uri:requestUri-A");
    when(mockRequestDetails.getHeaders()).thenReturn(requestHeaders);

    final RequestTagFactory requestTagFactory =
        new RequestTagFactory(mockCacheable, salientHeaderNames);
    final RequestTag requestTag =
        requestTagFactory.createTag(mockRequestDetails, mockAuthentication);
    assertEquals(
        new RequestTag(
            "uri:requestUri-A",
            Map.of(
                "x-single-value",
                List.of("singleValue"),
                "x-multi-values",
                List.of("multiValue1", "multiValue2")),
            Optional.of("cacheKey_A"),
            ""),
        requestTag);
  }

  @Test
  void testComputesCorrectTagForNoPrincipalAndMissingKey() {
    final Set<String> salientHeaderNames =
        Set.of("Y-Single-Value", "Y-Multi-Values", "Y-Not-Present");
    // RequestDetails.getHeaders() returns a Map<String, List<String>> where the keys are
    // lower-cased.
    final Map<String, List<String>> requestHeaders =
        Map.of(
            "y-single-value",
            List.of("singleValue"),
            "y-multi-values",
            List.of("multiValue1", "multiValue2"),
            "y-other",
            List.of("otherValue"));

    when(mockDatabase.getCacheKey()).thenReturn(Optional.empty());
    when(mockRequestDetails.getCompleteUrl()).thenReturn("uri:requestUri-B");
    when(mockRequestDetails.getHeaders()).thenReturn(requestHeaders);

    final RequestTagFactory requestTagFactory =
        new RequestTagFactory(
            mockDatabase,
            createServerConfiguration(
                new ArrayList<>(salientHeaderNames), Collections.emptyList()));

    final RequestTag requestTag =
        requestTagFactory.createTag(mockRequestDetails, mockAuthentication);
    assertEquals(
        new RequestTag(
            "uri:requestUri-B",
            Map.of(
                "y-single-value",
                List.of("singleValue"),
                "y-multi-values",
                List.of("multiValue1", "multiValue2")),
            Optional.empty(),
            ""),
        requestTag);
  }

  static ServerConfiguration createServerConfiguration(
      final List<String> varyHeaders, final List<String> excludeVary) {
    final AsyncConfiguration asyncConfiguration = new AsyncConfiguration();
    asyncConfiguration.setEnabled(true);
    asyncConfiguration.setVaryHeadersExcludedFromCacheKey(excludeVary);

    final HttpServerCachingConfiguration httpServerCachingConfiguration =
        new HttpServerCachingConfiguration();
    httpServerCachingConfiguration.setVary(varyHeaders);

    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    serverConfiguration.setAsync(asyncConfiguration);
    serverConfiguration.setHttpCaching(httpServerCachingConfiguration);
    return serverConfiguration;
  }
}
