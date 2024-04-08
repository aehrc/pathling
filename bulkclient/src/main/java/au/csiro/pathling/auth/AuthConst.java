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

import lombok.experimental.UtilityClass;

/**
 * Constants for authentication.
 */
@UtilityClass
public class AuthConst {

  /**
   * The client ID parameter.
   */
  public static final String PARAM_CLIENT_ID = "client_id";
  /**
   * The client secret parameter.
   */
  public static final String PARAM_CLIENT_SECRET = "client_secret";
  /**
   * The client assertion type parameter.
   */
  public static final String PARAM_CLIENT_ASSERTION_TYPE = "client_assertion_type";
  /**
   * The client assertion parameter.
   */
  public static final String PARAM_CLIENT_ASSERTION = "client_assertion";
  /**
   * The scope parameter.
   */
  public static final String PARAM_SCOPE = "scope";
  /**
   * The grant type parameter.
   */
  public static final String PARAM_GRANT_TYPE = "grant_type";
  /**
   * The grant type for client credentials.
   */
  public static final String GRANT_TYPE_CLIENT_CREDENTIALS = "client_credentials";
  /**
   * The grant type for refresh token.
   */
  public static final String CLIENT_ASSERTION_TYPE_JWT_BEARER = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
  /**
   * The grant type for refresh token.
   */
  public static final String AUTH_BASIC = "Basic";
  /**
   * The grant type for refresh token.
   */
  public static final String AUTH_BEARER = "Bearer";
}
