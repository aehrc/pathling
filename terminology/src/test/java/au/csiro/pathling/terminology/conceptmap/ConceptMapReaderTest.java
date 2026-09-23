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

package au.csiro.pathling.terminology.conceptmap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.terminology.local.VersionResolver;
import ca.uhn.fhir.context.FhirContext;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link ConceptMapReader} over a WireMock terminology server: the summary search and its
 * exact parameters, the version choice for pinned and unpinned references, paging, the exact set of
 * requests made, and the mapping of search and read failures.
 *
 * @author John Grimes
 */
class ConceptMapReaderTest {

  private static final String URL = "http://example.org/ConceptMap/sct-to-icd10";
  private static final String OTHER_URL = "http://example.org/ConceptMap/other";
  private static final String SNOMED = "http://snomed.info/sct";
  private static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";
  private static final String SEARCH_PATH = "/fhir/ConceptMap";
  private static final String READ_PATH_PATTERN = "/fhir/ConceptMap/.+";
  private static final String FHIR_JSON = "application/fhir+json";
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();
  private static WireMockServer wireMockServer;
  private static CloseableHttpClient httpClient;
  private static TerminologyClient terminologyClient;
  private static ConceptMapReader reader;

  @BeforeAll
  static void beforeAll() {
    wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMockServer.start();
    httpClient = HttpClients.createDefault();
    terminologyClient = buildClient(serverUrl());
    reader = new ConceptMapReader(terminologyClient, new VersionResolver(null));
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
  private static String encode(@Nonnull final Resource resource) {
    return FHIR_CONTEXT.newJsonParser().encodeResourceToString(resource);
  }

  /** Builds a summary of a map as a search returns it: id, url and version only. */
  @Nonnull
  private static ConceptMap summary(
      @Nonnull final String id, @Nonnull final String url, @Nullable final String version) {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setId(id);
    conceptMap.setUrl(url);
    if (version != null) {
      conceptMap.setVersion(version);
    }
    return conceptMap;
  }

  /** Builds the full resource of a map with one mapping whose target code names the version. */
  @Nonnull
  private static ConceptMap full(
      @Nonnull final String id, @Nullable final String version, @Nonnull final String targetCode) {
    final ConceptMap conceptMap = summary(id, URL, version);
    final ConceptMapGroupComponent group = conceptMap.addGroup();
    group.setSource(SNOMED);
    group.setTarget(ICD10);
    group
        .addElement()
        .setCode("22298006")
        .addTarget()
        .setCode(targetCode)
        .setEquivalence(ConceptMapEquivalence.EQUIVALENT);
    return conceptMap;
  }

  @Nonnull
  private static Bundle searchset(@Nonnull final ConceptMap... entries) {
    final Bundle bundle = new Bundle();
    bundle.setType(BundleType.SEARCHSET);
    for (final ConceptMap entry : entries) {
      bundle
          .addEntry()
          .setFullUrl(serverUrl() + "/ConceptMap/" + entry.getIdElement().getIdPart())
          .setResource(entry);
    }
    return bundle;
  }

  @Nonnull
  private static MappingBuilder searchRequest() {
    return get(urlPathEqualTo(SEARCH_PATH)).withQueryParam("url", equalTo(URL));
  }

  private static void stubSearch(@Nonnull final Bundle bundle) {
    wireMockServer.stubFor(
        searchRequest()
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(encode(bundle))));
  }

  private static void stubRead(@Nonnull final ConceptMap conceptMap) {
    wireMockServer.stubFor(
        get(urlPathEqualTo(SEARCH_PATH + "/" + conceptMap.getIdElement().getIdPart()))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(encode(conceptMap))));
  }

  private static void stubSearchOutcome(final int status, @Nonnull final String diagnostics) {
    wireMockServer.stubFor(
        searchRequest()
            .willReturn(
                aResponse()
                    .withStatus(status)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(encode(outcome(diagnostics)))));
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

  @Nonnull
  private static List<LoggedRequest> searchRequests() {
    return wireMockServer.findAll(getRequestedFor(urlPathEqualTo(SEARCH_PATH)));
  }

  @Nonnull
  private static List<LoggedRequest> readRequests() {
    return wireMockServer.findAll(getRequestedFor(urlPathMatching(READ_PATH_PATTERN)));
  }

  @Nonnull
  private static List<String> readPaths() {
    return readRequests().stream().map(request -> request.getUrl()).toList();
  }

  @Nonnull
  private static String queryValue(
      @Nonnull final LoggedRequest request, @Nonnull final String key) {
    return request.getQueryParams().get(key).firstValue();
  }

  @Test
  void pinnedSearchesWithVersionThenReadsTheChosenMap() {
    final ConceptMap map2026 = full("map-2026", "2026", "I21");
    stubSearch(searchset(summary("map-2026", URL, "2026")));
    stubRead(map2026);

    final Optional<ConceptMapContent> result = reader.read(URL, "2026", NO_LIMIT);

    assertEquals(ConceptMapContent.fromResource(map2026, NO_LIMIT), result.orElseThrow());
    final List<LoggedRequest> searches = searchRequests();
    assertEquals(1, searches.size());
    assertEquals(Set.of("url", "version", "_summary"), searches.get(0).getQueryParams().keySet());
    assertEquals(URL, queryValue(searches.get(0), "url"));
    assertEquals("2026", queryValue(searches.get(0), "version"));
    assertEquals("true", queryValue(searches.get(0), "_summary"));
    assertEquals(List.of("/fhir/ConceptMap/map-2026"), readPaths());
  }

  @Test
  void unpinnedSingleEntryIsRead() {
    final ConceptMap map = full("map-only", null, "I21");
    stubSearch(searchset(summary("map-only", URL, null)));
    stubRead(map);

    final Optional<ConceptMapContent> result = reader.read(URL, null, NO_LIMIT);

    assertEquals(ConceptMapContent.fromResource(map, NO_LIMIT), result.orElseThrow());
    final LoggedRequest search = searchRequests().get(0);
    assertEquals(Set.of("url", "_summary"), search.getQueryParams().keySet());
    assertEquals(List.of("/fhir/ConceptMap/map-only"), readPaths());
  }

  @Test
  void pinnedAnsweredOnlyWithAnotherVersionIsEmpty() {
    // A server that ignores the version search parameter returns the maps of every version.
    stubSearch(searchset(summary("map-2025", URL, "2025")));
    stubRead(full("map-2025", "2025", "I22"));

    final Optional<ConceptMapContent> result = reader.read(URL, "2026", NO_LIMIT);

    assertTrue(result.isEmpty());
    assertEquals("2026", queryValue(searchRequests().get(0), "version"));
    assertTrue(readRequests().isEmpty());
  }

  @Test
  void unpinnedSeveralVersionsReadsTheLatestOnly() {
    final ConceptMap map2026 = full("map-2026", "2026", "I21");
    stubSearch(searchset(summary("map-2025", URL, "2025"), summary("map-2026", URL, "2026")));
    stubRead(full("map-2025", "2025", "I22"));
    stubRead(map2026);

    final ConceptMapContent content = reader.read(URL, null, NO_LIMIT).orElseThrow();

    assertEquals("2026", content.getVersion());
    assertEquals("I21", content.getMappings().get(0).getTargetCode());
    assertEquals(List.of("/fhir/ConceptMap/map-2026"), readPaths());
  }

  @Test
  void unpinnedWithUndeterminableOrderIsVersionException() {
    // SemVer build metadata does not take part in precedence, so these two versions tie.
    stubSearch(
        searchset(summary("map-a", URL, "1.0.0+build1"), summary("map-b", URL, "1.0.0+build2")));

    final ConceptMapVersionException e =
        assertThrows(ConceptMapVersionException.class, () -> reader.read(URL, null, NO_LIMIT));

    assertTrue(e.getMessage().contains("Found more than one resource"), e.getMessage());
    assertTrue(e.getMessage().contains(URL), e.getMessage());
    assertTrue(readRequests().isEmpty());
  }

  @Test
  void unpinnedWithAVersionMissingIsVersionException() {
    stubSearch(searchset(summary("map-2026", URL, "2026"), summary("map-none", URL, null)));

    final ConceptMapVersionException e =
        assertThrows(ConceptMapVersionException.class, () -> reader.read(URL, null, NO_LIMIT));

    assertTrue(e.getMessage().contains("a version was missing"), e.getMessage());
    assertTrue(readRequests().isEmpty());
  }

  @Test
  void pinnedWithTwoEntriesIsVersionException() {
    stubSearch(searchset(summary("map-a", URL, "2026"), summary("map-b", URL, "2026")));

    final ConceptMapVersionException e =
        assertThrows(ConceptMapVersionException.class, () -> reader.read(URL, "2026", NO_LIMIT));

    assertTrue(e.getMessage().contains("2 ConceptMaps"), e.getMessage());
    assertTrue(e.getMessage().contains(URL), e.getMessage());
    assertTrue(e.getMessage().contains("2026"), e.getMessage());
    assertTrue(readRequests().isEmpty());
  }

  @Test
  void followsNextLink() {
    final String nextUrl = serverUrl() + "/ConceptMap?_page=2";
    final Bundle first = searchset(summary("map-2025", URL, "2025"));
    first.addLink().setRelation(Bundle.LINK_NEXT).setUrl(nextUrl);
    stubSearch(first);
    wireMockServer.stubFor(
        get(urlPathEqualTo(SEARCH_PATH))
            .withQueryParam("_page", equalTo("2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(encode(searchset(summary("map-2026", URL, "2026"))))));
    stubRead(full("map-2026", "2026", "I21"));

    final ConceptMapContent content = reader.read(URL, null, NO_LIMIT).orElseThrow();

    assertEquals("2026", content.getVersion());
    assertEquals(2, searchRequests().size());
    assertEquals(List.of("/fhir/ConceptMap/map-2026"), readPaths());
  }

  @Test
  void ignoresEntryWhoseUrlDiffers() {
    stubSearch(searchset(summary("other", OTHER_URL, "2027"), summary("map-2026", URL, "2026")));
    stubRead(full("map-2026", "2026", "I21"));

    final ConceptMapContent content = reader.read(URL, null, NO_LIMIT).orElseThrow();

    assertEquals("2026", content.getVersion());
    assertEquals(List.of("/fhir/ConceptMap/map-2026"), readPaths());
  }

  @Test
  void onlyEntriesWithAnotherUrlIsEmpty() {
    stubSearch(searchset(summary("other", OTHER_URL, "2027")));

    assertTrue(reader.read(URL, null, NO_LIMIT).isEmpty());
    assertTrue(readRequests().isEmpty());
  }

  @Test
  void emptySearchsetIsEmpty() {
    stubSearch(searchset());

    assertTrue(reader.read(URL, null, NO_LIMIT).isEmpty());
    assertTrue(readRequests().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 404, 405, 501})
  void searchFailureThatMeansNoConceptMapIsEmpty(final int status) {
    stubSearchOutcome(status, "Not supported");

    assertTrue(reader.read(URL, null, NO_LIMIT).isEmpty());
    assertTrue(readRequests().isEmpty());
  }

  @Test
  void searchServerErrorIsLookupExceptionCarryingDiagnostics() {
    stubSearchOutcome(500, "The database is unavailable");

    final ConceptMapLookupException e =
        assertThrows(ConceptMapLookupException.class, () -> reader.read(URL, null, NO_LIMIT));

    assertEquals(
        "the terminology server returned HTTP 500: The database is unavailable", e.getMessage());
  }

  @Test
  void searchConnectionFailureIsLookupExceptionNamingServerUrl() throws IOException {
    final String unreachable;
    try (final ServerSocket socket = new ServerSocket(0)) {
      unreachable = "http://localhost:" + socket.getLocalPort() + "/fhir";
    }
    final TerminologyClient client = buildClient(unreachable);
    final ConceptMapReader unreachableReader =
        new ConceptMapReader(client, new VersionResolver(null));

    final ConceptMapLookupException e =
        assertThrows(
            ConceptMapLookupException.class, () -> unreachableReader.read(URL, null, NO_LIMIT));

    assertEquals("terminology server " + unreachable + " could not be reached", e.getMessage());
    client.close();
  }

  @Test
  void readServerErrorIsContentException() {
    stubSearch(searchset(summary("map-2026", URL, "2026")));
    wireMockServer.stubFor(
        get(urlPathEqualTo(SEARCH_PATH + "/map-2026"))
            .willReturn(
                aResponse()
                    .withStatus(500)
                    .withHeader("Content-Type", FHIR_JSON)
                    .withBody(encode(outcome("Cannot read")))));

    final ConceptMapContentException e =
        assertThrows(ConceptMapContentException.class, () -> reader.read(URL, null, NO_LIMIT));

    assertEquals("the terminology server returned HTTP 500: Cannot read", e.getMessage());
  }

  @Test
  void readConnectionFailureIsContentException() {
    stubSearch(searchset(summary("map-2026", URL, "2026")));
    wireMockServer.stubFor(
        get(urlPathEqualTo(SEARCH_PATH + "/map-2026"))
            .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

    final ConceptMapContentException e =
        assertThrows(ConceptMapContentException.class, () -> reader.read(URL, null, NO_LIMIT));

    assertEquals("terminology server " + serverUrl() + " could not be reached", e.getMessage());
  }
}
