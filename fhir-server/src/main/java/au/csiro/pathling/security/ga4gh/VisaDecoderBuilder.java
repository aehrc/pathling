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

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.JWKS_URI;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.AuthorizationConfiguration.Ga4ghPassports;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.security.PathlingJwtDecoderBuilder;
import com.nimbusds.jwt.JWTClaimsSet;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
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
  public JwtDecoder build(@Nonnull final ServerConfiguration configuration) {
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
