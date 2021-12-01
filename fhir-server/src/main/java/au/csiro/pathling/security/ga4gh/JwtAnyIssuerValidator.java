/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import java.util.Collection;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;

/**
 * A JWT validator that can validate the issuer claim against a set of allowed issuers.
 *
 * @author John Grimes
 */
public class JwtAnyIssuerValidator implements OAuth2TokenValidator<Jwt> {

  private final JwtClaimValidator<Object> validator;

  /**
   * @param issuers the list of issuers that are allowable
   */
  public JwtAnyIssuerValidator(@Nonnull final Collection<String> issuers) {
    checkArgument(issuers != null && !issuers.isEmpty(), "issuers cannot be null or empty");

    final Predicate<Object> testClaimValue = (claimValue) -> (claimValue != null)
        && issuers.contains(
        claimValue.toString());
    this.validator = new JwtClaimValidator<>(JwtClaimNames.ISS, testClaimValue);
  }

  @Override
  public OAuth2TokenValidatorResult validate(@Nullable final Jwt token) {
    checkArgument(token != null, "token cannot be null");
    return this.validator.validate(token);
  }

}
