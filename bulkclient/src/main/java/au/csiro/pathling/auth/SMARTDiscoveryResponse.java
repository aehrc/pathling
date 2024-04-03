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

import au.csiro.pathling.export.utils.WebUtils;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Value;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

@Value
public class SMARTDiscoveryResponse {

  private static final Gson GSON = new GsonBuilder()
      .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
      .create();

  public static String SMART_WELL_KNOWN_CONFIGURATION_PATH = ".well-known/smart-configuration";

  @Nonnull
  String tokenEndpoint;

  @Nonnull
  List<String> grantTypesSupported = Collections.emptyList();

  @Nonnull
  List<String> tokenEndpointAuthMethodsSupported = Collections.emptyList();

  @Nonnull
  List<String> tokenEndpointAuthSigningAlgValuesSupported = Collections.emptyList();

  @Nonnull
  List<String> capabilities = Collections.emptyList();

  public static SMARTDiscoveryResponse get(@Nonnull final URI fhirEndpointURI,
      @Nonnull final HttpClient httpClient) throws IOException {
    final URI wellKnownUri = WebUtils.ensurePathEndsWithSlash(fhirEndpointURI).resolve(
        SMART_WELL_KNOWN_CONFIGURATION_PATH);
    final HttpGet request = new HttpGet(wellKnownUri);
    request.addHeader("Accept", "application/json");
    final HttpResponse response = httpClient.execute(request);
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      throw new IOException("Failed to fetch SMART configuration: " + response.getStatusLine());
    }
    return GSON.fromJson(EntityUtils.toString(response.getEntity()),
        SMARTDiscoveryResponse.class);
  }
}

