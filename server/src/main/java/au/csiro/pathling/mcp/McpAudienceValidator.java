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

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Requires that an access token was issued for the MCP endpoint, by checking that its audience
 * includes the endpoint's resource URL. MCP clients request tokens for that URL using the RFC 8707
 * {@code resource} parameter, and the MCP authorisation specification requires the server to refuse
 * tokens issued for any other resource.
 *
 * <p>A trailing slash is ignored on both sides, since clients and authorisation servers disagree on
 * it and it does not name a different resource.
 *
 * @author John Grimes
 * @see <a
 *     href="https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization#token-audience-binding-and-validation">MCP
 *     authorization: token audience binding and validation</a>
 */
public class McpAudienceValidator implements OAuth2TokenValidator<Jwt> {

  private static final OAuth2Error WRONG_AUDIENCE =
      new OAuth2Error("invalid_token", "The token was not issued for this MCP endpoint", null);

  @Nonnull private final Supplier<String> expectedAudience;

  /**
   * Creates a validator for the given audience.
   *
   * @param expectedAudience supplies the resource URL of the MCP endpoint, which may depend on the
   *     current request
   */
  public McpAudienceValidator(@Nonnull final Supplier<String> expectedAudience) {
    this.expectedAudience = expectedAudience;
  }

  @Override
  public OAuth2TokenValidatorResult validate(@Nonnull final Jwt token) {
    final String expected = withoutTrailingSlash(expectedAudience.get());
    final List<String> audiences = token.getAudience();
    final boolean matches =
        audiences != null
            && audiences.stream()
                .map(McpAudienceValidator::withoutTrailingSlash)
                .anyMatch(expected::equals);
    return matches
        ? OAuth2TokenValidatorResult.success()
        : OAuth2TokenValidatorResult.failure(WRONG_AUDIENCE);
  }

  @Nonnull
  private static String withoutTrailingSlash(@Nonnull final String value) {
    return value.replaceAll("/+$", "");
  }
}
