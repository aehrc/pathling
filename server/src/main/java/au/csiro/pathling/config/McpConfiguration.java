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

package au.csiro.pathling.config;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.ToString;

/**
 * Configuration for the Model Context Protocol (MCP) server, which exposes read-only query tools at
 * {@code /mcp}.
 *
 * @author John Grimes
 */
@Data
@ToString(doNotUseGetters = true)
public class McpConfiguration {

  /** Enables the MCP endpoint. */
  private boolean enabled = false;

  /**
   * The public URL of the MCP endpoint, as users enter it into their MCP client. It is advertised
   * as the {@code resource} in the protected resource metadata and is the audience required of
   * access tokens. When absent, it is derived from each request, honouring the {@code Forwarded}
   * and {@code X-Forwarded-*} headers.
   */
  @Nullable private String resourceUrl;

  /** The largest number of resources that the search tool will return. */
  @Min(1)
  private int maxSearchCount = 100;

  /** The largest number of rows that the view and SQL tools will return. */
  @Min(1)
  private int maxRows = 1000;

  /** Authorisation settings for the MCP endpoint, used when {@code pathling.auth} is enabled. */
  @Valid @NotNull private Auth auth = new Auth();

  /**
   * Returns the configured resource URL.
   *
   * @return the resource URL, or empty if it is to be derived from the request
   */
  @Nonnull
  public Optional<String> getResourceUrl() {
    return Optional.ofNullable(resourceUrl);
  }

  /** Authorisation settings for the MCP endpoint. */
  @Data
  public static class Auth {

    /**
     * The issuer of access tokens accepted at the MCP endpoint. Defaults to {@code
     * pathling.auth.issuer}.
     */
    @Nullable private String issuer;

    /**
     * The scopes advertised in the protected resource metadata, which MCP clients request when they
     * sign in.
     */
    @NotNull private List<String> scopesSupported = new ArrayList<>();

    /**
     * Returns the configured issuer.
     *
     * @return the issuer, or empty if the server's main issuer is to be used
     */
    @Nonnull
    public Optional<String> getIssuer() {
      return Optional.ofNullable(issuer);
    }
  }
}
