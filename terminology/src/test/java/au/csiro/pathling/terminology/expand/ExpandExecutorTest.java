/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology.expand;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.fhir.TerminologyClient;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ExpandExecutor} over a WireMock terminology server: paging by returned count,
 * completion with and without {@code total}, the exact request parameters for the canonical and
 * resource forms, deduplication across pages, and the mapping of HTTP failures.
 *
 * @author John Grimes
 */
class ExpandExecutorTest {

  private static final String URL = "http://example.org/ValueSet/cardiovascular-disease";
  private static final String SNOMED = "http://snomed.info/sct";
  private static final String EXPAND_PATH = "/fhir/ValueSet/$expand";
  private static final String FHIR_JSON = "application/fhir+json";
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();
  private static WireMockServer wireMockServer;
  private static CloseableHttpClient httpClient;
  private static TerminologyClient terminologyClient;
  private static ExpandExecutor executor;

  @BeforeAll
  static void beforeAll() {
    wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMockServer.start();
    httpClient = HttpClients.createDefault();
    terminologyClient = buildClient(serverUrl());
    executor = new ExpandExecutor(terminologyClient);
  }

  @AfterEach
  void tearDown() {
    wireMockServer.resetAll();
  }

  @AfterAll
  static void afterAll() throws IOException {
    terminologyClient.close();
    httpClient.close();
    wireMockServer.stop();
  }

  @Nonnull
  private static String serverUrl() {
    return "http://localhost:" + wireMockServer.port() + "/fhir";
  }

  @Nonnull
  private static TerminologyClient buildClient(@Nonnull final String serverUrl) {
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .serverUrl(serverUrl)
            .client(HttpClientConfiguration.builder().socketTimeout(5_000).build())
            .cache(HttpClientCachingConfiguration.builder().enabled(false).build())
            .authentication(TerminologyAuthConfiguration.builder().enabled(false).build())
            .build();
    return TerminologyClient.build(FHIR_CONTEXT, configuration, httpClient);
  }

  @Nonnull
  private static ValueSetExpansionContainsComponent entry(
      @Nonnull final String system, @Nonnull final String code) {
    final ValueSetExpansionContainsComponent entry = new ValueSetExpansionContainsComponent();
    entry.setSystem(system);
    entry.setCode(code);
    return entry;
  }

  @Nonnull
  private static ValueSetMember member(@Nonnull final String system, @Nonnull final String code) {
    return new ValueSetMember(system, null, code, null, null);
  }

  /** Builds a page of an expansion holding the given codes from SNOMED CT. */
  @Nonnull
  private static String page(
      @Nullable final Integer total, final int offset, @Nonnull final String... codes) {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(URL);
    final ValueSetExpansionComponent expansion = valueSet.getExpansion();
    expansion.setOffset(offset);
    if (total != null) {
      expansion.setTotal(total);
    }
    for (final String code : codes) {
      expansion.addContains(entry(SNOMED, code));
    }
    return encode(valueSet);
  }

  @Nonnull
  private static String encode(@Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    final IParser parser = FHIR_CONTEXT.newJsonParser();
    return parser.encodeResourceToString(resource);
  }

  private static void stubPage(final int offset, @Nonnull final String body) {
    wireMockServer.stubFor(
        get(urlPathEqualTo(EXPAND_PATH))
            .withQueryParam("offset", equalTo(String.valueOf(offset)))
            .willReturn(
                aResponse().withStatus(200).withHeader("Content-Type", FHIR_JSON).withBody(body)));
  }

  @Nonnull
  private static List<LoggedRequest> expandRequests() {
    return wireMockServer.findAll(getRequestedFor(urlPathEqualTo(EXPAND_PATH)));
  }

  @Nonnull
  private static Set<String> queryKeys(@Nonnull final LoggedRequest request) {
    return request.getQueryParams().keySet();
  }

  @Nonnull
  private static String queryValue(
      @Nonnull final LoggedRequest request, @Nonnull final String key) {
    final Map<String, QueryParameter> params = request.getQueryParams();
    return params.get(key).firstValue();
  }

  @Test
  void singlePageIsComplete() {
    stubPage(0, page(2, 0, "1", "2"));

    final Optional<ValueSetExpansion> result = executor.expand(URL, null, NO_LIMIT);

    assertTrue(result.isPresent());
    assertEquals(List.of(member(SNOMED, "1"), member(SNOMED, "2")), result.get().getMembers());
    assertEquals(1, expandRequests().size());
  }

  @Test
  void advancesSecondPageByReturnedCount() {
    stubPage(0, page(5, 0, "1", "2", "3"));
    stubPage(3, page(5, 3, "4", "5"));

    final Optional<ValueSetExpansion> result = executor.expand(URL, null, NO_LIMIT);

    assertTrue(result.isPresent());
    assertEquals(
        List.of(
            member(SNOMED, "1"),
            member(SNOMED, "2"),
            member(SNOMED, "3"),
            member(SNOMED, "4"),
            member(SNOMED, "5")),
        result.get().getMembers());
    final List<LoggedRequest> requests = expandRequests();
    assertEquals(2, requests.size());
    assertEquals("0", queryValue(requests.get(0), "offset"));
    assertEquals("3", queryValue(requests.get(1), "offset"));
    assertEquals(
        String.valueOf(ExpandExecutor.EXPAND_PAGE_SIZE), queryValue(requests.get(0), "count"));
  }

  @Test
  void stopsOnShortPageWhenTotalAbsent() {
    stubPage(0, page(null, 0, "1", "2"));

    final Optional<ValueSetExpansion> result = executor.expand(URL, null, NO_LIMIT);

    assertTrue(result.isPresent());
    assertEquals(2, result.get().getMembers().size());
    assertEquals(1, expandRequests().size());
  }

  @Test
  void stopsOnEmptyPageWhenTotalAbsent() {
    // A full page with no total is followed by another request, which returns nothing.
    final String[] codes = new String[ExpandExecutor.EXPAND_PAGE_SIZE];
    for (int i = 0; i < codes.length; i++) {
      codes[i] = String.valueOf(i);
    }
    stubPage(0, page(null, 0, codes));
    stubPage(ExpandExecutor.EXPAND_PAGE_SIZE, page(null, ExpandExecutor.EXPAND_PAGE_SIZE));

    final Optional<ValueSetExpansion> result = executor.expand(URL, null, NO_LIMIT);

    assertTrue(result.isPresent());
    assertEquals(ExpandExecutor.EXPAND_PAGE_SIZE, result.get().getMembers().size());
    assertEquals(2, expandRequests().size());
  }

  @Test
  void sendsPinnedVersionAsValueSetVersion() {
    stubPage(0, page(1, 0, "1"));

    executor.expand(URL, "2026", NO_LIMIT);

    final LoggedRequest request = expandRequests().get(0);
    assertEquals(Set.of("url", "valueSetVersion", "count", "offset"), queryKeys(request));
    assertEquals(URL, queryValue(request, "url"));
    assertEquals("2026", queryValue(request, "valueSetVersion"));
  }

  @Test
  void sendsNoVersionWhenUnpinned() {
    stubPage(0, page(1, 0, "1"));

    executor.expand(URL, null, NO_LIMIT);

    final LoggedRequest request = expandRequests().get(0);
    assertEquals(Set.of("url", "count", "offset"), queryKeys(request));
    assertEquals(URL, queryValue(request, "url"));
  }

  @Test
  void postsResourceWithOnlyValueSetCountAndOffset() {
    final ValueSet supplied = new ValueSet();
    supplied.setUrl(URL);
    supplied.getCompose().addInclude().setSystem(SNOMED).addConcept().setCode("22298006");
    wireMockServer.stubFor(
        post(urlPathEqualTo(EXPAND_PATH))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(page(1, 0, "22298006"))));

    final ValueSetExpansion result = executor.expand(supplied, NO_LIMIT);

    assertEquals(List.of(member(SNOMED, "22298006")), result.getMembers());
    final List<LoggedRequest> requests =
        wireMockServer.findAll(postRequestedFor(urlPathEqualTo(EXPAND_PATH)));
    assertEquals(1, requests.size());
    assertTrue(requests.get(0).getQueryParams().isEmpty());
    final Parameters body =
        (Parameters) FHIR_CONTEXT.newJsonParser().parseResource(requests.get(0).getBodyAsString());
    assertEquals(
        List.of("valueSet", "count", "offset"),
        body.getParameter().stream()
            .map(Parameters.ParametersParameterComponent::getName)
            .collect(Collectors.toList()));
    assertEquals(URL, ((ValueSet) body.getParameter().get(0).getResource()).getUrl());
    assertEquals(
        String.valueOf(ExpandExecutor.EXPAND_PAGE_SIZE),
        body.getParameter().get(1).getValue().primitiveValue());
    assertEquals("0", body.getParameter().get(2).getValue().primitiveValue());
  }

  @Test
  void usesSuppliedExpansionWithoutRequest() {
    final ValueSet supplied = new ValueSet();
    supplied.setUrl(URL);
    supplied.getExpansion().addContains(entry(SNOMED, "1"));

    final ValueSetExpansion result = executor.expand(supplied, NO_LIMIT);

    assertEquals(List.of(member(SNOMED, "1")), result.getMembers());
    assertTrue(wireMockServer.getAllServeEvents().isEmpty());
  }

  @Test
  void deduplicatesMembersAcrossPages() {
    stubPage(0, page(4, 0, "1", "2"));
    stubPage(2, page(4, 2, "2", "3"));

    final Optional<ValueSetExpansion> result = executor.expand(URL, null, NO_LIMIT);

    assertTrue(result.isPresent());
    assertEquals(
        List.of(member(SNOMED, "1"), member(SNOMED, "2"), member(SNOMED, "3")),
        result.get().getMembers());
  }

  @Test
  void carriesProvenanceFromFirstPage() {
    final ValueSet first = new ValueSet();
    first.setUrl(URL);
    first.setVersion("2026");
    final ValueSetExpansionComponent expansion = first.getExpansion();
    expansion.setIdentifier("urn:uuid:first");
    expansion.setTimestampElement(new org.hl7.fhir.r4.model.DateTimeType("2026-09-22T10:00:00Z"));
    expansion.setTotal(2);
    expansion.setOffset(0);
    expansion.addParameter().setName("version").setValue(new UriType(SNOMED + "|20260101"));
    expansion.addContains(entry(SNOMED, "1"));
    stubPage(0, encode(first));
    final ValueSet second = new ValueSet();
    second.setUrl(URL);
    second.getExpansion().setIdentifier("urn:uuid:second").setTotal(2).setOffset(1);
    second.getExpansion().addContains(entry(SNOMED, "2"));
    stubPage(1, encode(second));

    final ValueSetExpansion result = executor.expand(URL, "2026", NO_LIMIT).orElseThrow();

    assertEquals(URL, result.getUrl());
    assertEquals("2026", result.getVersion());
    assertEquals("urn:uuid:first", result.getIdentifier());
    assertEquals("2026-09-22T10:00:00Z", result.getTimestamp());
    assertEquals(List.of(SNOMED + "|20260101"), result.getCodeSystemVersions());
    assertEquals(2, result.getMembers().size());
  }

  @Test
  void returnsEmptyForNotFound() {
    wireMockServer.stubFor(
        get(urlPathEqualTo(EXPAND_PATH))
            .willReturn(
                aResponse()
                    .withStatus(404)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(encode(outcome("Unable to find ValueSet")))));

    final Optional<ValueSetExpansion> result = executor.expand(URL, null, NO_LIMIT);

    assertFalse(result.isPresent());
  }

  @Test
  void mapsUnprocessableEntityWithOutcomeToExceptionCarryingDiagnostics() {
    wireMockServer.stubFor(
        get(urlPathEqualTo(EXPAND_PATH))
            .willReturn(
                aResponse()
                    .withStatus(422)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(encode(outcome("The value set is too large to expand")))));

    final ValueSetExpansionException e =
        assertThrows(ValueSetExpansionException.class, () -> executor.expand(URL, null, NO_LIMIT));

    assertTrue(e.getMessage().contains("The value set is too large to expand"), e.getMessage());
  }

  @Test
  void mapsServerErrorWithoutOutcomeToExceptionNamingStatus() {
    wireMockServer.stubFor(
        get(urlPathEqualTo(EXPAND_PATH)).willReturn(aResponse().withStatus(500).withBody("boom")));

    final ValueSetExpansionException e =
        assertThrows(ValueSetExpansionException.class, () -> executor.expand(URL, null, NO_LIMIT));

    assertTrue(e.getMessage().contains("500"), e.getMessage());
  }

  @Test
  void mapsConnectionFailureToExceptionNamingServerUrl() throws IOException {
    final String unreachable;
    try (final java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
      unreachable = "http://localhost:" + socket.getLocalPort() + "/fhir";
    }
    final TerminologyClient client = buildClient(unreachable);
    final ExpandExecutor unreachableExecutor = new ExpandExecutor(client);

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class,
            () -> unreachableExecutor.expand(URL, null, NO_LIMIT));

    assertTrue(e.getMessage().contains(unreachable), e.getMessage());
    assertTrue(e.getMessage().contains("could not be reached"), e.getMessage());
    client.close();
  }

  @Test
  void stopsAfterPageThatRevealsExcess() {
    stubPage(0, page(6, 0, "1", "2", "3"));
    stubPage(3, page(6, 3, "4", "5", "6"));

    final ExpansionLimitExceededException e =
        assertThrows(ExpansionLimitExceededException.class, () -> executor.expand(URL, null, 2));

    assertEquals(2, e.getLimit());
    assertEquals(1, expandRequests().size());
  }

  @Test
  void acceptsExactlyMaxMembersAcrossPages() {
    stubPage(0, page(4, 0, "1", "2"));
    stubPage(2, page(4, 2, "3", "4"));

    final Optional<ValueSetExpansion> result = executor.expand(URL, null, 4);

    assertEquals(4, result.orElseThrow().getMembers().size());
  }

  @Test
  void rejectsPageWithoutExpansion() {
    final ValueSet noExpansion = new ValueSet();
    noExpansion.setUrl(URL);
    stubPage(0, encode(noExpansion));

    final ValueSetExpansionException e =
        assertThrows(ValueSetExpansionException.class, () -> executor.expand(URL, null, NO_LIMIT));

    assertTrue(e.getMessage().contains("no expansion"), e.getMessage());
  }

  @Nonnull
  private static OperationOutcome outcome(@Nonnull final String diagnostics) {
    final OperationOutcome outcome = new OperationOutcome();
    outcome
        .addIssue()
        .setSeverity(IssueSeverity.ERROR)
        .setCode(IssueType.PROCESSING)
        .setDiagnostics(diagnostics);
    return outcome;
  }
}
