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

package au.csiro.pathling.auth;

import org.apache.http.auth.Credentials;
import javax.annotation.Nonnull;
import java.security.Principal;

/**
 * Credentials for a token-based authentication method.
 */
public interface TokenCredentials extends Credentials {


  @Override
  default Principal getUserPrincipal() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  default String getPassword() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the token to be use for authentication.
   *
   * @return the token
   */
  @Nonnull
  Token getToken();
}
