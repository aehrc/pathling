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

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/**
 * Response from the token endpoint when using client credentials grant.
 */
@Value
@Builder
public class ClientCredentialsResponse {

  @Nullable
  @ToString.Exclude
  String accessToken;

  @Nullable
  String tokenType;

  int expiresIn;

  @Nullable
  String scope;

}
