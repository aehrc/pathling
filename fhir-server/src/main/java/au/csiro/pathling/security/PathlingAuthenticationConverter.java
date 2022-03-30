/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import java.util.Collection;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
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
@Profile("server & !ga4gh")
@Slf4j
public class PathlingAuthenticationConverter extends JwtAuthenticationConverter {

  /**
   * Creates a new instance.
   */
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
