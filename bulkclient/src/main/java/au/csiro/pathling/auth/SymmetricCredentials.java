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

import lombok.Value;
import org.apache.http.auth.Credentials;
import org.apache.http.message.BasicNameValuePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
public class SymmetricCredentials implements Credentials {

  @Nonnull
  String tokenEndpoint;

  @Nonnull
  String clientId;

  @Nonnull
  String clientSecret;

  @Nullable
  String scope;
  
  @Nonnull
  String toAuthString() {
    return Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(
        StandardCharsets.US_ASCII));
  }

  @Nonnull
  List<BasicNameValuePair> toFormParams() {
    return Stream.of(new BasicNameValuePair("client_id", clientId),
            new BasicNameValuePair("client_secret", clientSecret))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Principal getUserPrincipal() {
    return null;
  }

  @Override
  public String getPassword() {
    return null;
  }
}
