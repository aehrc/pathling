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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.config.McpConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

/**
 * Tests for {@link McpResourceUrl}, which decides the URL that the MCP endpoint identifies itself
 * by, and where its protected resource metadata is published.
 *
 * @author John Grimes
 */
class McpResourceUrlTest {

  // -- resolve --

  @Test
  void configuredUrlTakesPrecedenceOverTheRequest() {
    // An operator-supplied URL is used verbatim, whatever host the request arrived on.
    final McpConfiguration config = new McpConfiguration();
    config.setResourceUrl("https://pathling.example.com/mcp");
    final MockHttpServletRequest request = request("http", "internal-host", 8080);

    assertThat(McpResourceUrl.resolve(config, request))
        .isEqualTo("https://pathling.example.com/mcp");
  }

  @Test
  void derivedUrlUsesTheRequestSchemeHostAndPort() {
    final MockHttpServletRequest request = request("http", "localhost", 8080);

    assertThat(McpResourceUrl.resolve(new McpConfiguration(), request))
        .isEqualTo("http://localhost:8080/mcp");
  }

  @Test
  void derivedUrlOmitsTheDefaultPort() {
    // Clients compare the resource URL as a string, so a default port must not be spelled out.
    final MockHttpServletRequest request = request("https", "pathling.example.com", 443);

    assertThat(McpResourceUrl.resolve(new McpConfiguration(), request))
        .isEqualTo("https://pathling.example.com/mcp");
  }

  @Test
  void derivedUrlHonoursForwardedHeaders() {
    // Behind a TLS-terminating proxy the request arrives over plain HTTP on an internal host, and
    // the public URL is reconstructed from the X-Forwarded-* headers.
    final MockHttpServletRequest request = request("http", "pathling-svc", 8080);
    request.addHeader("X-Forwarded-Proto", "https");
    request.addHeader("X-Forwarded-Host", "pathling.example.com");
    request.addHeader("X-Forwarded-Port", "443");

    assertThat(McpResourceUrl.resolve(new McpConfiguration(), request))
        .isEqualTo("https://pathling.example.com/mcp");
  }

  // -- metadataUrl --

  @Test
  void metadataUrlInsertsTheWellKnownSegmentBeforeThePath() {
    // RFC 9728 section 3.1: the well-known segment goes between the host and the path.
    assertThat(McpResourceUrl.metadataUrl("https://pathling.example.com/mcp"))
        .isEqualTo("https://pathling.example.com/.well-known/oauth-protected-resource/mcp");
  }

  @Test
  void metadataUrlPreservesPortAndPathPrefix() {
    assertThat(McpResourceUrl.metadataUrl("http://localhost:8080/pathling/mcp"))
        .isEqualTo("http://localhost:8080/.well-known/oauth-protected-resource/pathling/mcp");
  }

  private static MockHttpServletRequest request(
      final String scheme, final String host, final int port) {
    final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/mcp");
    request.setScheme(scheme);
    request.setServerName(host);
    request.setServerPort(port);
    return request;
  }
}
