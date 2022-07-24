/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.JWKS_URI;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.AuthorizationConfiguration.Ga4ghPassports;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.PathlingJwtDecoderBuilder;
import com.nimbusds.jwt.JWTClaimsSet;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("server & ga4gh")
public class VisaDecoderBuilder extends PathlingJwtDecoderBuilder {

  /**
   * @param oidcConfiguration configuration used to instantiate the builder
   */
  public VisaDecoderBuilder(@Nonnull final OidcConfiguration oidcConfiguration) {
    super(oidcConfiguration);
  }

  @Override
  public JwtDecoder build(@Nonnull final Configuration configuration) {
    final AuthorizationConfiguration auth = getAuthConfiguration(configuration);
    final Ga4ghPassports ga4ghPassports = auth.getGa4ghPassports();

    // The issuer within the token is validated to ensure that it is in the allowed list.
    final List<OAuth2TokenValidator<Jwt>> validators = new ArrayList<>();
    validators.add(new JwtAnyIssuerValidator(ga4ghPassports.getAllowedVisaIssuers()));
    return buildDecoderWithValidators(validators);
  }

  @Nonnull
  @Override
  protected String getJwksUri(@Nonnull final JWTClaimsSet claimsSet) {
    // In this implementation, we get the JWKS URI using the value of the issuer claim within the
    // token, rather than some preconfigured issuer.
    final String issuer = claimsSet.getIssuer();
    final OidcConfiguration oidcConfiguration = new OidcConfiguration(issuer);
    return checkPresent(oidcConfiguration.get(JWKS_URI));
  }

}
