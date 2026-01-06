/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.ToString;

/** Represents configuration specific to authorisation. */
@Data
@ToString(doNotUseGetters = true)
public class AuthorizationConfiguration {

  /** Enables authorization. */
  @NotNull private boolean enabled;

  /**
   * Configures the issuing domain for bearer tokens, which will be checked against the claims
   * within incoming bearer tokens.
   */
  @Nullable private String issuer;

  /**
   * Configures the audience for bearer tokens, which is the FHIR endpoint that tokens are intended
   * to be authorised for.
   */
  @Nullable private String audience;

  /**
   * The list of SMART on FHIR capabilities to advertise in the SMART configuration document.
   * Defaults to {@code ["launch-standalone"]} for backward compatibility.
   *
   * @see <a href="https://hl7.org/fhir/smart-app-launch/conformance.html">SMART App Launch
   *     Conformance</a>
   */
  @Nonnull private List<String> capabilities = Collections.singletonList("launch-standalone");

  /**
   * Returns the configured issuer.
   *
   * @return the issuer, or empty if not configured
   */
  @Nonnull
  public Optional<String> getIssuer() {
    return Optional.ofNullable(issuer);
  }

  /**
   * Returns the configured audience.
   *
   * @return the audience, or empty if not configured
   */
  @Nonnull
  public Optional<String> getAudience() {
    return Optional.ofNullable(audience);
  }

  /**
   * Returns the configured SMART capabilities.
   *
   * @return the list of SMART capabilities to advertise
   */
  @Nonnull
  public List<String> getCapabilities() {
    return capabilities;
  }
}
