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

package au.csiro.pathling.config;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.ToString;

/**
 * Represents configuration specific to authorization.
 */
@Data
@ToString(doNotUseGetters = true)
public class AuthorizationConfiguration {

  /**
   * Enables authorization.
   */
  @NotNull
  private boolean enabled;

  /**
   * Configures the issuing domain for bearer tokens, which will be checked against the claims
   * within incoming bearer tokens.
   */
  @Nullable
  private String issuer;

  /**
   * Configures the audience for bearer tokens, which is the FHIR endpoint that tokens are intended
   * to be authorized for.
   */
  @Nullable
  private String audience;

  @NotNull
  private Ga4ghPassports ga4ghPassports;

  @Nonnull
  public Optional<String> getIssuer() {
    return Optional.ofNullable(issuer);
  }

  @Nonnull
  public Optional<String> getAudience() {
    return Optional.ofNullable(audience);
  }

  /**
   * Configuration relating to support for GA4GH Passports as a method of authorization.
   */
  @Data
  public static class Ga4ghPassports {

    @NotNull
    private String patientIdSystem;

    /**
     * A set of allowable visa issuers.
     */
    @NotNull
    private List<String> allowedVisaIssuers;

  }

}
