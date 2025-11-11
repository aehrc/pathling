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

import jakarta.annotation.Nullable;
import lombok.Data;

/**
 * Represents configuration specific to ping and pull import functionality.
 *
 * @author John Grimes
 */
@Data
public class PnpConfiguration {

  /**
   * The client identifier for SMART Backend Services authentication.
   */
  @Nullable
  private String clientId;

  /**
   * The token endpoint URL for obtaining access tokens. If not specified, the client will attempt
   * to discover it via the SMART configuration endpoint.
   */
  @Nullable
  private String tokenEndpoint;

  /**
   * The private key in JWK format for asymmetric authentication (RS384).
   */
  @Nullable
  private String privateKeyJwk;

  /**
   * The client secret for symmetric authentication.
   */
  @Nullable
  private String clientSecret;

  /**
   * The requested scope for authentication (e.g., "system/*.read").
   */
  @Nullable
  private String scope;

  /**
   * The minimum number of seconds that a token should have before expiry when deciding whether to
   * use it. If a cached token has less than this many seconds until expiry, a new token will be
   * requested. Defaults to 120 seconds if not specified.
   */
  @Nullable
  private Long tokenExpiryTolerance;

  /**
   * The directory where files will be downloaded during ping and pull import operations. This
   * location should have sufficient space for large FHIR exports. Defaults to
   * /usr/share/staging/pnp if not specified.
   */
  @Nullable
  private String downloadLocation;

  /**
   * The file extension to filter for when processing downloaded files. Defaults to ".ndjson" if not
   * specified.
   */
  @Nullable
  private String fileExtension;

}
