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
import java.util.Optional;
import lombok.Data;
import lombok.ToString;

/**
 * Configuration for the admin UI.
 *
 * @author John Grimes
 */
@Data
@ToString(doNotUseGetters = true)
public class AdminUiConfiguration {

  /**
   * The OAuth client ID for the admin UI. When set, this value is included in the SMART
   * configuration response at {@code /.well-known/smart-configuration} as {@code
   * admin_ui_client_id}.
   */
  @Nullable private String clientId;

  /**
   * Returns the client ID as an optional.
   *
   * @return an optional containing the client ID, or empty if not configured
   */
  @Nonnull
  public Optional<String> getClientId() {
    return Optional.ofNullable(clientId);
  }
}
