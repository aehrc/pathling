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

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Tests for {@link McpAudienceValidator}, which requires an access token to have been issued for
 * the MCP endpoint itself.
 *
 * @author John Grimes
 */
class McpAudienceValidatorTest {

  private static final String RESOURCE_URL = "https://pathling.example.com/mcp";

  private final McpAudienceValidator validator = new McpAudienceValidator(() -> RESOURCE_URL);

  @Test
  void acceptsTokenIssuedForTheResourceUrl() {
    assertThat(validator.validate(tokenWithAudience(RESOURCE_URL)).hasErrors()).isFalse();
  }

  @Test
  void acceptsTokenWhenOnlyATrailingSlashDiffers() {
    // Authorisation servers and clients disagree on the trailing slash; it does not name a
    // different resource.
    assertThat(validator.validate(tokenWithAudience(RESOURCE_URL + "/")).hasErrors()).isFalse();
    assertThat(
            new McpAudienceValidator(() -> RESOURCE_URL + "/")
                .validate(tokenWithAudience(RESOURCE_URL))
                .hasErrors())
        .isFalse();
  }

  @Test
  void acceptsTokenListingSeveralAudiences() {
    final Jwt jwt = token(List.of("https://pathling.example.com/fhir", RESOURCE_URL));

    assertThat(validator.validate(jwt).hasErrors()).isFalse();
  }

  @Test
  void rejectsTokenIssuedForAnotherResource() {
    // A token for the FHIR API is not a token for the MCP endpoint.
    final OAuth2TokenValidatorResult result =
        validator.validate(tokenWithAudience("https://pathling.example.com/fhir"));

    assertThat(result.hasErrors()).isTrue();
    assertThat(result.getErrors()).anyMatch(error -> "invalid_token".equals(error.getErrorCode()));
  }

  @Test
  void rejectsTokenWithoutAudience() {
    assertThat(validator.validate(token(List.of())).hasErrors()).isTrue();
  }

  private static Jwt tokenWithAudience(final String audience) {
    return token(List.of(audience));
  }

  private static Jwt token(final List<String> audiences) {
    final Jwt.Builder builder =
        Jwt.withTokenValue("token")
            .header("alg", "RS256")
            .subject("user")
            .issuedAt(Instant.now())
            .expiresAt(Instant.now().plusSeconds(60));
    if (!audiences.isEmpty()) {
      builder.audience(audiences);
    }
    return builder.build();
  }
}
