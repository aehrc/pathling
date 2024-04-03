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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHttpRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TokenAuthRequestInterceptorTest {

  @Mock
  CredentialsProvider credentialsProvider;

  @Mock
  TokenCredentials tokenCredentials;
  final HttpClientContext httpContext = HttpClientContext.create();

  @Test
  void testAddsAuthHeaderWhenTokenAvailable() throws HttpException, IOException {

    final HttpHost host = HttpHost.create("http://foo.bar");
    final TokenAuthRequestInterceptor tokenAuthRequestInterceptor = new TokenAuthRequestInterceptor();
    final BasicHttpRequest request = new BasicHttpRequest("GET", "http://foo.bar/");
    httpContext.setCredentialsProvider(credentialsProvider);
    httpContext.setTargetHost(host);
    when(credentialsProvider.getCredentials(eq(new AuthScope(host)))).thenReturn(tokenCredentials);
    when(tokenCredentials.getToken()).thenReturn("XYZ");
    tokenAuthRequestInterceptor.process(request, httpContext);
    assertEquals("Bearer XYZ", request.getFirstHeader("Authorization").getValue());
  }


  @Test
  void testNoAuthHeaderWhenNoCredentialsForHost() throws HttpException, IOException {

    final HttpHost host = HttpHost.create("http://foo.bar");
    final TokenAuthRequestInterceptor tokenAuthRequestInterceptor = new TokenAuthRequestInterceptor();
    final BasicHttpRequest request = new BasicHttpRequest("GET", "http://foo.bar/");
    httpContext.setCredentialsProvider(credentialsProvider);
    httpContext.setTargetHost(host);
    when(credentialsProvider.getCredentials(eq(new AuthScope(host)))).thenReturn(null);
    tokenAuthRequestInterceptor.process(request, httpContext);
    assertNull(request.getFirstHeader("Authorization"));
  }

  @Test
  void testNoAuthHeaderWhenNoCredentialProvider() throws HttpException, IOException {
    final HttpHost host = HttpHost.create("http://foo.bar");
    final TokenAuthRequestInterceptor tokenAuthRequestInterceptor = new TokenAuthRequestInterceptor();
    final BasicHttpRequest request = new BasicHttpRequest("GET", "http://foo.bar/");
    httpContext.setTargetHost(host);
    tokenAuthRequestInterceptor.process(request, httpContext);
    assertNull(request.getFirstHeader("Authorization"));
  }
}
