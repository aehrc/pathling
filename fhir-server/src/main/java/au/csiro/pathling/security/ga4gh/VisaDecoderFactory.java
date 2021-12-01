/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.JWKS_URI;
import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Authorization;
import au.csiro.pathling.Configuration.Authorization.Ga4ghPassports;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.PathlingJwtDecoderFactory;
import com.nimbusds.jwt.JWTClaimsSet;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

/**
 * A JWT decoder specifically for visa tokens within GA4GH passports. This decoder does not validate
 * audience, and validates issuer against a list of allowed issuers within configuration.
 *
 * @author John Grimes
 */
public class VisaDecoderFactory extends PathlingJwtDecoderFactory {

  /**
   * @param oidcConfiguration used to get the JWKS URI
   */
  public VisaDecoderFactory(@Nonnull final OidcConfiguration oidcConfiguration) {
    super(oidcConfiguration);
  }

  @Override
  public JwtDecoder createDecoder(@Nullable final Configuration configuration) {
    final Authorization auth = getAuthConfiguration(configuration);
    final Ga4ghPassports ga4ghPassports = auth.getGa4ghPassports();
    check(ga4ghPassports.isEnabled());

    // The issuer within the token is validated to ensure that it is in the allowed list.
    final List<OAuth2TokenValidator<Jwt>> validators = new ArrayList<>();
    validators.add(new JwtAnyIssuerValidator(ga4ghPassports.getAllowedIssuers()));
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
