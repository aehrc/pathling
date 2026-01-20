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

package au.csiro.pathling.interceptors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.cache.CachingHttpClients;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

/**
 * Fetches and caches OIDC discovery documents from an issuer's well-known endpoint. Uses Apache
 * HttpClient 5's built-in HTTP caching to respect Cache-Control and Expires headers.
 *
 * @author John Grimes
 */
@Slf4j
public class OidcDiscoveryFetcher {

  private static final String WELL_KNOWN_PATH = "/.well-known/openid-configuration";
  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

  @Nonnull private final String discoveryUrl;
  @Nonnull private final CloseableHttpClient httpClient;
  @Nonnull private final Gson gson;

  /**
   * Constructs a new OidcDiscoveryFetcher for the given issuer.
   *
   * @param issuer the OIDC issuer URL
   */
  public OidcDiscoveryFetcher(@Nonnull final String issuer) {
    this(issuer, CachingHttpClients.createMemoryBound());
  }

  /**
   * Constructs a new OidcDiscoveryFetcher with a custom HTTP client. This constructor is primarily
   * for testing purposes.
   *
   * @param issuer the OIDC issuer URL
   * @param httpClient the HTTP client to use for requests
   */
  public OidcDiscoveryFetcher(
      @Nonnull final String issuer, @Nonnull final CloseableHttpClient httpClient) {
    this.discoveryUrl = issuer + WELL_KNOWN_PATH;
    this.httpClient = httpClient;
    this.gson = new Gson();
  }

  /**
   * Fetches the OIDC discovery document, using cached response if available and valid.
   *
   * @return the discovery document as a map, or empty if fetching failed
   */
  @Nonnull
  public Optional<Map<String, Object>> fetch() {
    final HttpGet request = new HttpGet(discoveryUrl);
    request.addHeader("Accept", "application/json");

    try {
      return httpClient.execute(
          request,
          (final ClassicHttpResponse response) -> {
            final int statusCode = response.getCode();
            if (statusCode != 200) {
              log.warn(
                  "Failed to fetch OIDC discovery document from {}: HTTP {}",
                  discoveryUrl,
                  statusCode);
              return Optional.empty();
            }

            final HttpEntity entity = response.getEntity();
            if (entity == null) {
              log.warn("OIDC discovery document response from {} had no body", discoveryUrl);
              return Optional.empty();
            }

            try {
              final String body = EntityUtils.toString(entity);
              final Map<String, Object> document = gson.fromJson(body, MAP_TYPE);
              return Optional.of(document);
            } catch (final ParseException e) {
              log.warn("Failed to parse OIDC discovery document from {}: {}", discoveryUrl, e);
              return Optional.empty();
            }
          });
    } catch (final IOException e) {
      log.warn("Failed to fetch OIDC discovery document from {}: {}", discoveryUrl, e.getMessage());
      return Optional.empty();
    }
  }
}
