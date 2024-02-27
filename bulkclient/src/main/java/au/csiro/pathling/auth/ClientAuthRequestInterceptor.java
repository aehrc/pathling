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

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

@Slf4j
public class ClientAuthRequestInterceptor implements HttpRequestInterceptor {

  public static final String HTTP_AUTHORIZATION = "Authorization";

  public ClientAuthRequestInterceptor() {
  }

  @Override
  public void process(@Nonnull final HttpRequest request, @Nonnull final HttpContext context)
      throws HttpException, IOException {
    final Optional<String> maybeAccessToken = AuthContext.getToken();
    maybeAccessToken.ifPresent(
        accessToken -> {
          log.debug("Adding access token to request: {}", request.getRequestLine());
          request.addHeader(HTTP_AUTHORIZATION, "Bearer " + accessToken);
        });
  }
}
