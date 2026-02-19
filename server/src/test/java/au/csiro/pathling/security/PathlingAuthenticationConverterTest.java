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

package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Tests for {@link PathlingAuthenticationConverter} verifying that authorities are extracted from
 * the "authorities" claim in JWT tokens.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class PathlingAuthenticationConverterTest {

  @Test
  void extractsAuthoritiesFromAuthoritiesClaim() {
    // A JWT with an "authorities" claim should produce authorities without a prefix.
    final PathlingAuthenticationConverter converter = new PathlingAuthenticationConverter();

    final Jwt jwt =
        Jwt.withTokenValue("token")
            .header("alg", "RS256")
            .subject("user")
            .issuedAt(Instant.now())
            .expiresAt(Instant.now().plusSeconds(3600))
            .claim("authorities", List.of("pathling:read", "pathling:aggregate"))
            .build();

    final JwtAuthenticationToken authentication = (JwtAuthenticationToken) converter.convert(jwt);
    assertNotNull(authentication);

    final Collection<GrantedAuthority> authorities = authentication.getAuthorities();
    assertNotNull(authorities);

    // The authorities should match the claim values with no prefix added.
    final List<String> authorityStrings =
        authorities.stream().map(GrantedAuthority::getAuthority).toList();
    assertTrue(authorityStrings.contains("pathling:read"));
    assertTrue(authorityStrings.contains("pathling:aggregate"));
  }

  @Test
  void returnsEmptyAuthoritiesWhenClaimMissing() {
    // A JWT without an "authorities" claim should produce no authorities.
    final PathlingAuthenticationConverter converter = new PathlingAuthenticationConverter();

    final Jwt jwt =
        Jwt.withTokenValue("token")
            .header("alg", "RS256")
            .subject("user")
            .issuedAt(Instant.now())
            .expiresAt(Instant.now().plusSeconds(3600))
            .claim("scope", "openid")
            .build();

    final JwtAuthenticationToken authentication = (JwtAuthenticationToken) converter.convert(jwt);
    assertNotNull(authentication);

    // With no "authorities" claim, the authorities list should be empty.
    assertTrue(authentication.getAuthorities().isEmpty());
  }

  @Test
  void authoritiesHaveNoPrefix() {
    // The converter should be configured with an empty authority prefix.
    final PathlingAuthenticationConverter converter = new PathlingAuthenticationConverter();

    final Jwt jwt =
        Jwt.withTokenValue("token")
            .header("alg", "RS256")
            .subject("user")
            .issuedAt(Instant.now())
            .expiresAt(Instant.now().plusSeconds(3600))
            .claim("authorities", List.of("pathling:write"))
            .build();

    final JwtAuthenticationToken authentication = (JwtAuthenticationToken) converter.convert(jwt);

    // Authority should be exactly "pathling:write" with no "SCOPE_" or other prefix.
    final String authority = authentication.getAuthorities().iterator().next().getAuthority();
    assertTrue(authority.equals("pathling:write"));
  }
}
