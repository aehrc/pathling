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

package au.csiro.pathling.operations.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Shared infrastructure for the asynchronous export integration tests: kick-off, polling, result
 * retrieval and download, plus builders for the {@code Parameters} body and helpers for reading the
 * manifest back.
 *
 * @author John Grimes
 */
abstract class AbstractAsyncExportIT {

  @LocalServerPort protected int port;

  @Autowired protected WebTestClient webTestClient;

  protected final Gson gson = new Gson();

  @BeforeEach
  void setUpClient() {
    // The 60-second response timeout matches the other server integration tests; the 5-second
    // default is exceeded by Spark warmup on loaded CI runners.
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            .responseTimeout(Duration.ofSeconds(60))
            .build();
  }

  @Nonnull
  protected String systemLevelUri() {
    return "http://localhost:" + port + "/fhir/$sql-export";
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

  /** Kicks off an export, polls to completion, and returns the parsed manifest. */
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

  /** Kicks off an export, polls to completion, and returns the result URL from the 303. */
  @Nonnull
  protected String resultLocationOf(
      @Nonnull final String uri, @Nonnull final Map<String, Object> body)
      throws InterruptedException {
    final String contentLocation = contentLocationOf(uri, body);

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

  /** Kicks off an export and returns its status URL. */
  @Nonnull
  protected String contentLocationOf(
      @Nonnull final String uri, @Nonnull final Map<String, Object> body) {
    final String contentLocation =
        kickOff(uri, body)
            .expectStatus()
            .isAccepted()
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");
    assertThat(contentLocation).isNotNull();
    return contentLocation;
  }

  @Nonnull
  protected String download(@Nonnull final String location) {
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
    return new String(bytes == null ? new byte[0] : bytes, StandardCharsets.UTF_8);
  }

  /**
   * Downloads every file of a manifest output and concatenates their contents. A result spanning
   * several Spark partitions is written as one file per partition, and the manifest repeats the
   * {@code location} part once per file, so an assertion about the exported rows has to consider
   * all of them.
   *
   * @param output a single {@code output} parameter from the completion manifest
   * @return the concatenated contents of every file the output names
   */
  @Nonnull
  protected String downloadAll(@Nonnull final Map<String, Object> output) {
    final List<String> locations = partValues(output, "location", "valueUri");
    assertThat(locations).as("Expected the output to name at least one file").isNotEmpty();
    return locations.stream().map(this::download).collect(Collectors.joining());
  }

  // -------------------------------------------------------------------------
  // Request builders
  // -------------------------------------------------------------------------

  @SafeVarargs
  @Nonnull
  protected final Map<String, Object> parameters(@Nonnull final Map<String, Object>... params) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("resourceType", "Parameters");
    parameters.put("parameter", new ArrayList<>(List.of(params)));
    return parameters;
  }

  @SuppressWarnings("unchecked")
  protected void addParam(
      @Nonnull final Map<String, Object> parameters, @Nonnull final Map<String, Object> param) {
    ((List<Map<String, Object>>) parameters.get("parameter")).add(param);
  }

  @Nonnull
  protected Map<String, Object> simpleParam(
      @Nonnull final String name, @Nonnull final String valueKey, @Nonnull final Object value) {
    final Map<String, Object> param = new LinkedHashMap<>();
    param.put("name", name);
    param.put(valueKey, value);
    return param;
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

  /** Builds a {@code subject} repetition from the given parts. */
  @SafeVarargs
  @Nonnull
  protected final Map<String, Object> subject(@Nonnull final Map<String, Object>... parts) {
    final Map<String, Object> param = new LinkedHashMap<>();
    param.put("name", "subject");
    param.put("part", new ArrayList<>(List.of(parts)));
    return param;
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
