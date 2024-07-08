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

package au.csiro.pathling.fhir;

import au.csiro.pathling.PathlingVersion;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import jakarta.annotation.Nonnull;
import javax.annotation.Nullable;

@Interceptor
public class UserAgentInterceptor {

  public static final String PRODUCT_IDENTIFIER = "pathling";
  @Nonnull
  private final PathlingVersion version;

  public UserAgentInterceptor() {
    this.version = new PathlingVersion();
  }

  @SuppressWarnings("unused")
  @Hook(Pointcut.CLIENT_REQUEST)
  public void handleClientRequest(@Nullable final IHttpRequest httpRequest) {
    if (httpRequest != null) {
      final String userAgent = version.getDescriptiveVersion()
          .map(version -> PRODUCT_IDENTIFIER + "/" + version)
          .orElse(PRODUCT_IDENTIFIER);
      httpRequest.removeHeaders("User-Agent");
      httpRequest.addHeader("User-Agent", userAgent);
    }
  }

}
