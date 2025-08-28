/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhir;

import jakarta.annotation.Nullable;

/**
 * Represents an OAuth2 client credentials response.
 *
 * @param accessToken the access token returned by the authorization server
 * @param tokenType the type of token returned
 * @param expiresIn the lifetime in seconds of the access token
 * @param refreshToken the refresh token which can be used to obtain new access tokens
 * @param scope the scope of the access token
 * @author John Grimes
 */
public record ClientCredentialsResponse(
    @Nullable String accessToken,
    @Nullable String tokenType,
    int expiresIn,
    @Nullable String refreshToken,
    @Nullable String scope
) {

}
