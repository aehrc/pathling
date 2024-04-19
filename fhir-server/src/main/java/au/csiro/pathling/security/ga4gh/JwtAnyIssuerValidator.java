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

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.function.Predicate;
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
    checkArgument(!issuers.isEmpty(), "issuers cannot be empty");

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
