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

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;

/**
 * The implementation of the SMART symmetric client authentication profile.
 *
 * @see <a
 * href="https://build.fhir.org/ig/HL7/smart-app-launch/client-confidential-symmetric.html">Client
 * Authentication: Symmetric</a>
 */
@Value
@Builder
public class SymmetricClientAuthMethod implements ClientAuthMethod {

  @Nonnull
  String tokenEndpoint;

  @Nonnull
  String clientId;

  @Nonnull
  String clientSecret;

  @Nullable
  @Builder.Default
  String scope = null;

  @Builder.Default
  boolean sendClientCredentialsInBody = false;

  @Nonnull
  String toAuthString() {
    return Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(
        StandardCharsets.US_ASCII));
  }

  @Override
  @Nonnull
  public List<BasicNameValuePair> getAuthParams(@Nonnull final Instant __) {
    return sendClientCredentialsInBody
           ? List.of(new BasicNameValuePair(AuthConst.PARAM_CLIENT_ID, clientId),
        new BasicNameValuePair(AuthConst.PARAM_CLIENT_SECRET, clientSecret))
           : Collections.emptyList();
  }

  @Override
  @Nonnull
  public List<Header> getAuthHeaders() {
    return sendClientCredentialsInBody
           ? Collections.emptyList()
           : List.of(new BasicHeader(HttpHeaders.AUTHORIZATION,
               AuthConst.AUTH_BASIC + " " + toAuthString()));
  }
}
