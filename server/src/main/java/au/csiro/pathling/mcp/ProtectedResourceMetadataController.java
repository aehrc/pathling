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
import au.csiro.pathling.config.ServerConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Publishes the OAuth 2.0 protected resource metadata of the MCP endpoint, which tells an MCP
 * client which authorisation server issues tokens for it and which scopes to request.
 *
 * <p>The document is served at the location derived from the resource URL, and also at the root
 * well-known location, which clients probe when a challenge does not say where to look.
 *
 * @author John Grimes
 * @see <a href="https://www.rfc-editor.org/rfc/rfc9728">RFC 9728: OAuth 2.0 Protected Resource
 *     Metadata</a>
 */
@RestController
@ConditionalOnProperty(
    prefix = "pathling",
    name = {"auth.enabled", "mcp.enabled"},
    havingValue = "true")
public class ProtectedResourceMetadataController {

  @Nonnull private final ServerConfiguration configuration;

  /**
   * Constructs a new ProtectedResourceMetadataController.
   *
   * @param configuration the server configuration
   */
  public ProtectedResourceMetadataController(@Nonnull final ServerConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Returns the protected resource metadata.
   *
   * @param request the current request, from which the resource URL is derived when it is not
   *     configured
   * @return the metadata document
   */
  @GetMapping(
      value = {McpResourceUrl.METADATA_PATH, McpResourceUrl.METADATA_PATH + "/**"},
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Map<String, Object> metadata(@Nonnull final HttpServletRequest request) {
    final McpConfiguration mcp = configuration.getMcp();
    final Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("resource", McpResourceUrl.resolve(mcp, request));
    metadata.put("authorization_servers", List.of(McpSecurityConfiguration.issuer(configuration)));
    metadata.put("bearer_methods_supported", List.of("header"));
    if (!mcp.getAuth().getScopesSupported().isEmpty()) {
      metadata.put("scopes_supported", mcp.getAuth().getScopesSupported());
    }
    metadata.put("resource_name", "Pathling");
    return metadata;
  }
}
