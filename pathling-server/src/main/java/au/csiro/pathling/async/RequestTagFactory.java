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

package au.csiro.pathling.async;

import static java.util.function.Predicate.not;

import au.csiro.pathling.cache.Cacheable;
import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;


/**
 * Factory for creating {@link RequestTag} objects using the key of provided cacheable object.
 */
@Component
@Profile("server")
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Getter
public final class RequestTagFactory {

  private final Cacheable state;
  private final Set<String> salientHeaderNames;

  private static Set<String> getSalientHeaderNames(
      @Nonnull final ServerConfiguration configuration) {
    final Set<String> excludedVaryHeaders = new HashSet<>(
        configuration.getAsync().getVaryHeadersExcludedFromCacheKey());
    return configuration.getHttpCaching().getVary().stream()
        .filter(not(excludedVaryHeaders::contains))
        .collect(Collectors.toUnmodifiableSet());
  }

  RequestTagFactory(@Nonnull final Cacheable state, @Nonnull final Set<String> salientHeaderNames) {
    this.state = state;
    this.salientHeaderNames = salientHeaderNames.stream().map(String::toLowerCase)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Public constructor to use for Spring component bean creation.
   */
  @Autowired
  public RequestTagFactory(@Nonnull final CacheableDatabase database,
                           @Nonnull final ServerConfiguration configuration) {
    this(database, getSalientHeaderNames(configuration));
  }

  /**
   * Creates a {@link RequestTag} object from the provided request details.
   *
   * @param requestDetails the request details
   * @param authentication the authentication object
   * @return the request tag
   */
  @Nonnull
  public RequestTag createTag(@Nonnull final ServletRequestDetails requestDetails, @Nullable
  final Authentication authentication) {
    // TODO - implement auth correctly in E-Tag
    final Optional<String> currentCacheKey = state.getCacheKey();
    final Map<String, List<String>> salientHeaders = requestDetails.getHeaders().entrySet().stream()
        .filter(entry -> salientHeaderNames.contains(entry.getKey().toLowerCase()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    return new RequestTag(requestDetails.getCompleteUrl(), salientHeaders, currentCacheKey);
  }
}
