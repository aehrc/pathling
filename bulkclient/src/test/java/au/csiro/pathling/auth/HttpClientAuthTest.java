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

import static org.apache.http.auth.AuthScope.ANY_REALM;

import java.io.IOException;
import java.security.Principal;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

class HttpClientAuthTest {
  
  static class TokenCredentials implements Credentials {

    @Override
    public Principal getUserPrincipal() {
      return null;
    }

    @Override
    public String getPassword() {
      return null;
    }
  }
  


  static class TokenCredentialsRequestInterceptor implements org.apache.http.HttpRequestInterceptor {

    @Override
    public void process(org.apache.http.HttpRequest request, org.apache.http.protocol.HttpContext context)
        throws org.apache.http.HttpException, IOException {
      
      CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(
          org.apache.http.client.protocol.HttpClientContext.CREDS_PROVIDER);
      
      final Credentials credentials = credsProvider.getCredentials(new AuthScope((HttpHost) context.getAttribute(
          HttpClientContext.HTTP_TARGET_HOST), ANY_REALM, "AuthToken"));
      System.out.println("Adding token to request: "+ credentials);
      request.addHeader("Authorization", "Bearer " + "token");
    }
  }
  
  
  @Test
  void testGetTokenFromServer() throws IOException {


    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        new AuthScope("localhost", 8080, ANY_REALM, "AuthToken" ),
        new TokenCredentials());
    
    final CloseableHttpClient authHttpClient = HttpClients.custom()
        .setDefaultCredentialsProvider(credentialsProvider)
        .addInterceptorFirst(new TokenCredentialsRequestInterceptor())
        .build();


    authHttpClient.execute(new HttpGet("http://localhost:8080/"));
  }

}
