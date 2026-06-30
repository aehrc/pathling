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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Shared infrastructure for the {@code $sqlquery-export} integration tests: the async kick-off,
 * polling, result retrieval, and download helpers, plus the Parameters request builders and parsing
 * helpers. Concrete subclasses supply the {@code @SpringBootTest} configuration and the test
 * methods.
 *
 * @author John Grimes
 */
abstract class AbstractSqlQueryExportIT {

  @LocalServerPort protected int port;

  @Autowired protected WebTestClient webTestClient;

  protected final Gson gson = new Gson();

  @BeforeEach
  void setUpClient() {
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .build();
  }

  // -------------------------------------------------------------------------
  // Endpoints
  // -------------------------------------------------------------------------

  @Nonnull
  protected String systemLevelUri() {
    return "http://localhost:" + port + "/fhir/$sqlquery-export";
  }

  @Nonnull
  protected String typeLevelUri() {
    return "http://localhost:" + port + "/fhir/Library/$sqlquery-export";
  }

  @Nonnull
  protected String instanceLevelUri(@Nonnull final String libraryId) {
    return "http://localhost:" + port + "/fhir/Library/" + libraryId + "/$sqlquery-export";
  }

  // -------------------------------------------------------------------------
  // Async flow
  // -------------------------------------------------------------------------

  @Nonnull
  protected WebTestClient.ResponseSpec kickOff(
      @Nonnull final String uri, @Nonnull final Map<String, Object> body) {
    return webTestClient
        .post()
        .uri(uri)
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .bodyValue(gson.toJson(body))
        .exchange();
  }

  @Nonnull
  protected Map<String, Object> exportToCompletion(
      @Nonnull final String uri, @Nonnull final Map<String, Object> body)
      throws InterruptedException {
    final byte[] manifest =
        webTestClient
            .get()
            .uri(resultLocationOf(uri, body))
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return parse(manifest);
  }

  /** Kicks off an export, polls to completion, and returns the result URL from the 303 redirect. */
  @Nonnull
  protected String resultLocationOf(
      @Nonnull final String uri, @Nonnull final Map<String, Object> body)
      throws InterruptedException {
    final String contentLocation =
        kickOff(uri, body)
            .expectStatus()
            .isAccepted()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");
    assertThat(contentLocation).isNotNull();

    String resultLocation = null;
    for (int attempt = 0; attempt < 60 && resultLocation == null; attempt++) {
      final var poll =
          webTestClient
              .get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .returnResult(String.class);
      final HttpStatus status = (HttpStatus) poll.getStatus();
      if (status == HttpStatus.SEE_OTHER) {
        resultLocation = poll.getResponseHeaders().getFirst("Location");
      } else if (status == HttpStatus.ACCEPTED) {
        Thread.sleep(500);
      } else {
        throw new AssertionError("Unexpected poll status: " + status);
      }
    }
    assertThat(resultLocation).as("Expected a 303 result location within the timeout").isNotNull();
    return resultLocation;
  }

  /** Kicks off an export expected to fail, polling until the result URL returns an error status. */
  @Nonnull
  protected WebTestClient.ResponseSpec resultOfFailedExport(
      @Nonnull final String uri, @Nonnull final Map<String, Object> body)
      throws InterruptedException {
    final String contentLocation =
        kickOff(uri, body)
            .expectStatus()
            .isAccepted()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");
    assertThat(contentLocation).isNotNull();

    for (int attempt = 0; attempt < 60; attempt++) {
      final var poll =
          webTestClient
              .get()
              .uri(contentLocation)
              .header("Accept", "application/fhir+json")
              .exchange()
              .returnResult(String.class);
      final HttpStatus status = (HttpStatus) poll.getStatus();
      if (status == HttpStatus.SEE_OTHER) {
        final String resultLocation = poll.getResponseHeaders().getFirst("Location");
        return webTestClient
            .get()
            .uri(resultLocation)
            .header("Accept", "application/fhir+json")
            .exchange();
      } else if (status == HttpStatus.ACCEPTED) {
        Thread.sleep(500);
      } else {
        // The failure surfaced directly on the status poll.
        return webTestClient
            .get()
            .uri(contentLocation)
            .header("Accept", "application/fhir+json")
            .exchange();
      }
    }
    throw new AssertionError("Export neither completed nor failed within the timeout");
  }

  @Nonnull
  protected byte[] downloadBytes(@Nonnull final String location) {
    final byte[] bytes =
        webTestClient
            .get()
            .uri(location)
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return bytes == null ? new byte[0] : bytes;
  }

  @Nonnull
  protected String download(@Nonnull final String location) {
    return new String(downloadBytes(location), StandardCharsets.UTF_8);
  }

  // -------------------------------------------------------------------------
  // Request builders
  // -------------------------------------------------------------------------

  @Nonnull
  protected Map<String, Object> storedQuery(
      @Nonnull final String libraryId, @Nullable final String format) {
    final Map<String, Object> parameters = parametersWith(queryPartWithReference(libraryId));
    if (format != null) {
      addSimpleParam(parameters, "_format", "valueString", format);
    }
    return parameters;
  }

  @Nonnull
  protected Map<String, Object> queryPartWithReference(@Nonnull final String libraryId) {
    final Map<String, Object> queryParam = new LinkedHashMap<>();
    queryParam.put("name", "query");
    queryParam.put(
        "part", new ArrayList<>(List.of(referencePart("queryReference", "Library/" + libraryId))));
    return queryParam;
  }

  @Nonnull
  protected Map<String, Object> parametersWith(@Nonnull final Map<String, Object> param) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put("parameter", new ArrayList<>(List.of(param)));
    return parameters;
  }

  @Nonnull
  protected Map<String, Object> emptyParameters() {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put("parameter", new ArrayList<>());
    return parameters;
  }

  @SuppressWarnings("unchecked")
  protected void addParam(
      @Nonnull final Map<String, Object> parameters, @Nonnull final Map<String, Object> param) {
    ((List<Map<String, Object>>) parameters.get("parameter")).add(param);
  }

  @SuppressWarnings("unchecked")
  protected void addSimpleParam(
      @Nonnull final Map<String, Object> parameters,
      @Nonnull final String name,
      @Nonnull final String valueKey,
      @Nonnull final Object value) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put(valueKey, value);
    ((List<Map<String, Object>>) parameters.get("parameter")).add(part);
  }

  @Nonnull
  protected Map<String, Object> referencePart(
      @Nonnull final String name, @Nonnull final String reference) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put("valueReference", Map.of("reference", reference));
    return part;
  }

  @Nonnull
  protected Map<String, Object> resourcePart(
      @Nonnull final String name, @Nonnull final Map<String, Object> resource) {
    final Map<String, Object> part = new LinkedHashMap<>();
    part.put("name", name);
    part.put("resource", resource);
    return part;
  }

  // -------------------------------------------------------------------------
  // Parameters parsing
  // -------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Nonnull
  protected Map<String, Object> parse(@Nullable final byte[] body) {
    return gson.fromJson(
        new String(body == null ? new byte[0] : body, StandardCharsets.UTF_8), Map.class);
  }

  @SuppressWarnings("unchecked")
  @Nullable
  protected static Map<String, Object> findParam(
      @Nonnull final Map<String, Object> parameters, @Nonnull final String name) {
    final List<Map<String, Object>> list = (List<Map<String, Object>>) parameters.get("parameter");
    if (list == null) {
      return null;
    }
    return list.stream().filter(p -> name.equals(p.get("name"))).findFirst().orElse(null);
  }

  @Nullable
  protected static String findParamValue(
      @Nonnull final Map<String, Object> parameters,
      @Nonnull final String name,
      @Nonnull final String valueKey) {
    final Map<String, Object> param = findParam(parameters, name);
    return param == null ? null : (String) param.get(valueKey);
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  protected static List<Map<String, Object>> paramsByName(
      @Nonnull final Map<String, Object> parameters, @Nonnull final String name) {
    final List<Map<String, Object>> list = (List<Map<String, Object>>) parameters.get("parameter");
    final List<Map<String, Object>> result = new ArrayList<>();
    if (list != null) {
      for (final Map<String, Object> p : list) {
        if (name.equals(p.get("name"))) {
          result.add(p);
        }
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  protected static List<String> partValues(
      @Nonnull final Map<String, Object> param,
      @Nonnull final String partName,
      @Nonnull final String valueKey) {
    final List<Map<String, Object>> parts = (List<Map<String, Object>>) param.get("part");
    final List<String> values = new ArrayList<>();
    if (parts != null) {
      for (final Map<String, Object> p : parts) {
        if (partName.equals(p.get("name"))) {
          values.add((String) p.get(valueKey));
        }
      }
    }
    return values;
  }

  @Nullable
  protected static String partValue(
      @Nonnull final Map<String, Object> param,
      @Nonnull final String partName,
      @Nonnull final String valueKey) {
    final List<String> values = partValues(param, partName, valueKey);
    return values.isEmpty() ? null : values.get(0);
  }
}
