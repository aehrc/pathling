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

import au.csiro.pathling.config.AuthConfiguration;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class ClientAuthProviderTest {


  @Disabled
  @Test
  void testGetTokenFromServer() throws IOException {

    System.out.println("clientSecret: " + System.getProperty("pszul.clientSecret"));

    final AuthConfiguration smartCDR = AuthConfiguration.builder()
        .tokenEndpoint("https://aehrc-cdr.cc/smartsec_r4/oauth/token")
        .clientId("pathling-bulk-client")
        .clientSecret(System.getProperty("pszul.clientSecret"))
        .scope("system/*.read")
        .build();

    final SymetricAuthTokenProvider clientAuthProvider = new SymetricAuthTokenProvider(smartCDR, new AuthScope("aehrc-cdr.cc", -1));
    final Optional<String> token = clientAuthProvider.getToken(new AuthScope("aehrc-cdr.cc", -1));
    System.out.println(token);

    final CloseableHttpClient authHttpClient = HttpClients.custom()
        .addInterceptorFirst(new ClientAuthRequestInterceptor()).build();

    final URI url = URI.create("https://aehrc-cdr.cc/fhir_r4/$export?_type=Patient&_outputFormat=application%2Ffhir%2Bndjson");
    final HttpGet getMeta = new HttpGet(url);
    getMeta.addHeader("Prefer", "respond-async");  

    final CloseableHttpResponse response = clientAuthProvider.withToken(
        () -> authHttpClient.execute(getMeta));
    System.out.println(response);
    System.out.println(EntityUtils.toString(response.getEntity()));
  }

}
