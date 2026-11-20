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
package au.csiro.pathling.mcp;

import au.csiro.pathling.config.McpConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.util.ForwardedHeaderUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Decides the URL by which the MCP endpoint identifies itself as an OAuth protected resource, and
 * the location of its protected resource metadata.
 *
 * <p>The resource URL must match the URL that a user enters into their MCP client exactly, because
 * clients compare it as a string and authorisation servers issue tokens for it as the audience.
 *
 * @author John Grimes
 * @see <a href="https://www.rfc-editor.org/rfc/rfc9728">RFC 9728: OAuth 2.0 Protected Resource
 *     Metadata</a>
 */
public final class McpResourceUrl {

  /** The path at which the MCP endpoint is served. */
  public static final String ENDPOINT_PATH = "/mcp";

  /** The well-known path under which protected resource metadata is published. */
  public static final String METADATA_PATH = "/.well-known/oauth-protected-resource";

  private McpResourceUrl() {}

  /**
   * Returns the resource URL of the MCP endpoint: the configured URL if there is one, otherwise the
   * URL derived from the request, honouring the {@code Forwarded} and {@code X-Forwarded-*} headers
   * set by a reverse proxy.
   *
   * @param configuration the MCP configuration
   * @param request the current request
   * @return the resource URL, e.g. {@code https://pathling.example.com/mcp}
   */
  @Nonnull
  public static String resolve(
      @Nonnull final McpConfiguration configuration, @Nonnull final HttpServletRequest request) {
    return configuration.getResourceUrl().orElseGet(() -> derive(request));
  }

  /**
   * Returns the location of the protected resource metadata for a resource URL, formed as RFC 9728
   * section 3.1 describes by inserting the well-known segment between the host and the path.
   *
   * @param resourceUrl the resource URL
   * @return the metadata URL, e.g. {@code
   *     https://pathling.example.com/.well-known/oauth-protected-resource/mcp}
   */
  @Nonnull
  public static String metadataUrl(@Nonnull final String resourceUrl) {
    final UriComponents resource = UriComponentsBuilder.fromUriString(resourceUrl).build();
    final String path = resource.getPath() == null ? "" : resource.getPath();
    return UriComponentsBuilder.newInstance()
        .scheme(resource.getScheme())
        .host(resource.getHost())
        .port(resource.getPort())
        .path(METADATA_PATH + (path.equals("/") ? "" : path))
        .toUriString();
  }

  @Nonnull
  private static String derive(@Nonnull final HttpServletRequest request) {
    final ServletServerHttpRequest serverRequest = new ServletServerHttpRequest(request);
    final UriComponents base =
        ForwardedHeaderUtils.adaptFromForwardedHeaders(
                serverRequest.getURI(), serverRequest.getHeaders())
            .replacePath(request.getContextPath() + ENDPOINT_PATH)
            .replaceQuery(null)
            .fragment(null)
            .build();
    // A default port is left implicit, as a user would write the URL.
    final boolean defaultPort =
        ("http".equals(base.getScheme()) && base.getPort() == 80)
            || ("https".equals(base.getScheme()) && base.getPort() == 443);
    return defaultPort
        ? UriComponentsBuilder.newInstance().uriComponents(base).port(-1).toUriString()
        : base.toUriString();
  }
}
