/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
