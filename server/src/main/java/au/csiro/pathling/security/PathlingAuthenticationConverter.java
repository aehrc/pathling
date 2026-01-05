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

package au.csiro.pathling.security;

import jakarta.annotation.Nonnull;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.stereotype.Component;

/**
 * A simple authentication converter that gets the Pathling authorities from the authorities claim.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
@Slf4j
public class PathlingAuthenticationConverter extends JwtAuthenticationConverter {

  /** Creates a new instance. */
  public PathlingAuthenticationConverter() {
    log.debug("Instantiating authentication converter");
    setJwtGrantedAuthoritiesConverter(authoritiesConverter());
  }

  @Nonnull
  private static Converter<Jwt, Collection<GrantedAuthority>> authoritiesConverter() {
    final JwtGrantedAuthoritiesConverter converter = new JwtGrantedAuthoritiesConverter();
    converter.setAuthoritiesClaimName("authorities");
    converter.setAuthorityPrefix("");
    return converter;
  }
}
