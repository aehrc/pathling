/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.SNOMED_URI;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.editStub;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.fhir.ClientAuthInterceptor;
import au.csiro.pathling.fhir.ClientCredentialsResponse;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.ClientProtocolException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("NullableProblems")
@Slf4j
public class TerminologyAuthenticationTest {

  public static final int WIREMOCK_PORT = 4072;

  WireMockServer wireMockServer;
  TerminologyService terminologyService;
  Gson gson;
  StubMapping codeSystemSearchStub;
  StubMapping expandStub;
  StubMapping clientCredentialsStub;
  String clientCredentialsResponse;
  Set<SimpleCoding> codings;
  FhirContext fhirContext;

  @BeforeEach
  void setUp() {
    wireMockServer = new WireMockServer(
        new WireMockConfiguration().port(WIREMOCK_PORT));
    WireMock.configureFor("localhost", WIREMOCK_PORT);
    log.info("Starting WireMock server");
    wireMockServer.start();

    final TerminologyAuthConfiguration authConfig = new TerminologyAuthConfiguration();
    authConfig.setEnabled(true);
    authConfig.setTokenEndpoint("http://localhost:" + WIREMOCK_PORT
        + "/auth/realms/aehrc/protocol/openid-connect/token");
    authConfig.setClientId("some-client-id");
    authConfig.setClientSecret("some-client-secret");
    authConfig.setScope("openid");
    authConfig.setTokenExpiryTolerance(120L);

    fhirContext = FhirContext.forR4();
    final TerminologyClient terminologyClient = TerminologyClient.build(fhirContext,
        "http://localhost:" + WIREMOCK_PORT + "/fhir", 60_000, true,
        authConfig);
    terminologyService = new DefaultTerminologyService(fhirContext, terminologyClient,
        UUID::randomUUID);

    gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();

    final String codeSystemSearchResponse = getResourceAsString(
        "txResponses/TerminologyAuthenticationTest/codeSystemSearch.Bundle.json");
    codeSystemSearchStub = stubFor(get(urlMatching("/fhir/CodeSystem.*"))
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/fhir+json")
            .withBody(codeSystemSearchResponse)));

    final String expandResponse = getResourceAsString(
        "txResponses/TerminologyAuthenticationTest/intersect.ValueSet.json");
    expandStub = stubFor(post(urlMatching("/fhir/ValueSet.*"))
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/fhir+json")
            .withBody(expandResponse)));

    clientCredentialsResponse = gson.toJson(new ClientCredentialsResponse(
        "some-access-token", null, 3600, "some-refresh-token", null));
    clientCredentialsStub = stubFor(
        post(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
            .willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withBody(clientCredentialsResponse)
            ));

    codings = new HashSet<>(List.of(new SimpleCoding(SNOMED_URI, "48429009")));
  }

  @AfterEach
  void tearDown() {
    log.info("Stopping WireMock server");
    wireMockServer.stop();
    ClientAuthInterceptor.clearAccessContexts();
  }

  @Nonnull
  private Set<SimpleCoding> sendRequest() {
    return terminologyService.intersect(SNOMED_URI + "?fhir_vs", codings);
  }

  @Test
  void intersect() {
    final Set<SimpleCoding> result = sendRequest();
    assertEquals(codings, result);

    verify(1, postRequestedFor(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withHeader("Accept", equalTo("application/json"))
        .withRequestBody(equalTo(
            "grant_type=client_credentials&client_id=some-client-id&client_secret=some-client-secret&scope=openid")));
    verify(1, getRequestedFor(urlMatching("/fhir/CodeSystem.*"))
        .withHeader("Authorization", equalTo("Bearer some-access-token")));
    verify(1, postRequestedFor(urlMatching("/fhir/ValueSet.*"))
        .withHeader("Authorization", equalTo("Bearer some-access-token")));
  }

  @Test
  void withoutScope() {
    final TerminologyAuthConfiguration authConfig = new TerminologyAuthConfiguration();
    authConfig.setEnabled(true);
    authConfig.setTokenEndpoint("http://localhost:" + WIREMOCK_PORT
        + "/auth/realms/aehrc/protocol/openid-connect/token");
    authConfig.setClientId("some-client-id");
    authConfig.setClientSecret("some-client-secret");
    authConfig.setTokenExpiryTolerance(120L);

    final TerminologyClient terminologyClient = TerminologyClient.build(fhirContext,
        "http://localhost:" + WIREMOCK_PORT + "/fhir", 60_000, false,
        authConfig);
    terminologyService = new DefaultTerminologyService(fhirContext, terminologyClient,
        UUID::randomUUID);

    sendRequest();

    verify(1, postRequestedFor(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withHeader("Accept", equalTo("application/json"))
        .withRequestBody(equalTo(
            "grant_type=client_credentials&client_id=some-client-id&client_secret=some-client-secret")));
  }

  @Test
  void expired() throws InterruptedException {
    final TerminologyAuthConfiguration authConfig = new TerminologyAuthConfiguration();
    authConfig.setEnabled(true);
    authConfig.setTokenEndpoint("http://localhost:" + WIREMOCK_PORT
        + "/auth/realms/aehrc/protocol/openid-connect/token");
    authConfig.setClientId("some-client-id");
    authConfig.setClientSecret("some-client-secret");
    authConfig.setScope("openid");
    authConfig.setTokenExpiryTolerance(1L);

    fhirContext = FhirContext.forR4();
    final TerminologyClient terminologyClient = TerminologyClient.build(fhirContext,
        "http://localhost:" + WIREMOCK_PORT + "/fhir", 60_000, false,
        authConfig);
    terminologyService = new DefaultTerminologyService(fhirContext, terminologyClient,
        UUID::randomUUID);

    final ClientCredentialsResponse expiryLessThanToleranceResponse = new ClientCredentialsResponse(
        "some-access-token", null, 2, "some-refresh-token", null);
    editStub(post(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withId(clientCredentialsStub.getId())
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(gson.toJson(expiryLessThanToleranceResponse))));

    sendRequest();
    Thread.sleep(1000);
    sendRequest();

    verify(2, postRequestedFor(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withHeader("Accept", equalTo("application/json"))
        .withRequestBody(equalTo(
            "grant_type=client_credentials&client_id=some-client-id&client_secret=some-client-secret&scope=openid")));
  }

  @Test
  void noContentTypeHeader() {
    editStub(post(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withId(clientCredentialsStub.getId())
        .willReturn(aResponse().withBody(clientCredentialsResponse)));

    final InternalErrorException internalErrorException = assertThrows(InternalErrorException.class,
        this::sendRequest);
    assertNotNull(internalErrorException.getCause());
    assertEquals(ClientProtocolException.class, internalErrorException.getCause().getClass());
    assertEquals("Client credentials response contains no Content-Type header",
        internalErrorException.getCause().getMessage());
  }

  @Test
  void nonJsonContentType() {
    editStub(post(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withId(clientCredentialsStub.getId())
        .willReturn(aResponse()
            .withHeader("Content-Type", "text/plain")
            .withBody(clientCredentialsResponse)));

    final InternalErrorException internalErrorException = assertThrows(InternalErrorException.class,
        this::sendRequest);
    assertNotNull(internalErrorException.getCause());
    assertEquals(ClientProtocolException.class, internalErrorException.getCause().getClass());
    assertEquals("Invalid response from token endpoint: content type is not application/json",
        internalErrorException.getCause().getMessage());
  }

  @Test
  void missingAccessToken() {
    final ClientCredentialsResponse missingAccessTokenResponse = new ClientCredentialsResponse(null,
        null, 3600, "some-refresh-token", null);
    editStub(post(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withId(clientCredentialsStub.getId())
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(gson.toJson(missingAccessTokenResponse))));

    final InternalErrorException internalErrorException = assertThrows(InternalErrorException.class,
        this::sendRequest);
    assertNotNull(internalErrorException.getCause());
    assertEquals(ClientProtocolException.class, internalErrorException.getCause().getClass());
    assertEquals("Client credentials grant does not contain access token",
        internalErrorException.getCause().getMessage());
  }

  @Test
  void expiryLessThanTolerance() {
    final ClientCredentialsResponse expiryLessThanToleranceResponse = new ClientCredentialsResponse(
        "some-access-token", null, 1, "some-refresh-token", null);
    editStub(post(urlMatching("/auth/realms/aehrc/protocol/openid-connect/token.*"))
        .withId(clientCredentialsStub.getId())
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody(gson.toJson(expiryLessThanToleranceResponse))));

    final InternalErrorException internalErrorException = assertThrows(InternalErrorException.class,
        this::sendRequest);
    assertNotNull(internalErrorException.getCause());
    assertEquals(ClientProtocolException.class, internalErrorException.getCause().getClass());
    assertEquals("Client credentials grant expiry is less than the tolerance: 1",
        internalErrorException.getCause().getMessage());
  }

}
