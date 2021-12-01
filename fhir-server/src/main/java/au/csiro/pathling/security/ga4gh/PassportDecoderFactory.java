/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Authorization;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.PathlingJwtDecoderFactory;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.stereotype.Component;

/**
 * A JWT decoder for use with GA4GH passports.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = {"auth.enabled", "auth.ga4gh-passports.enabled"},
    havingValue = "true")
@Profile("server")
public class PassportDecoderFactory extends PathlingJwtDecoderFactory {

  /**
   * @param oidcConfiguration used to get the JWKS URI
   */
  public PassportDecoderFactory(@Nonnull final OidcConfiguration oidcConfiguration) {
    super(oidcConfiguration);
  }

  @Override
  public JwtDecoder createDecoder(@Nullable final Configuration configuration) {
    final Authorization auth = getAuthConfiguration(configuration);

    // The passport decoder is the same as the regular Pathling decoder with the exception that the
    // audience claim is not required.
    final List<OAuth2TokenValidator<Jwt>> validators = new ArrayList<>();
    auth.getIssuer().ifPresent(i -> validators.add(new JwtIssuerValidator(i)));
    return buildDecoderWithValidators(validators);
  }

}
