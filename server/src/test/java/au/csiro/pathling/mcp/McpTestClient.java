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

package au.csiro.pathling.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.test.web.reactive.server.WebTestClient.ResponseSpec;

/**
 * A minimal MCP client for tests, which posts JSON-RPC messages to the Streamable HTTP endpoint the
 * way an MCP client does.
 *
 * @author John Grimes
 */
final class McpTestClient {

  static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull private final WebTestClient client;

  @Nonnull private final String url;

  @Nullable private final String bearerToken;

  private final AtomicInteger nextId = new AtomicInteger(1);

  McpTestClient(
      @Nonnull final WebTestClient client, final int port, @Nullable final String bearerToken) {
    this.client =
        client
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
            .responseTimeout(Duration.ofSeconds(120))
            .build();
    this.url = "http://localhost:" + port + "/mcp";
    this.bearerToken = bearerToken;
  }

  /**
   * Posts a JSON-RPC request and returns the response for further assertions.
   *
   * @param method the JSON-RPC method
   * @param params the method parameters
   * @return the response
   */
  @Nonnull
  ResponseSpec post(@Nonnull final String method, @Nonnull final Map<String, Object> params) {
    final Map<String, Object> message = new LinkedHashMap<>();
    message.put("jsonrpc", "2.0");
    message.put("id", nextId.getAndIncrement());
    message.put("method", method);
    message.put("params", params);
    return client
        .post()
        .uri(url)
        .contentType(MediaType.APPLICATION_JSON)
        .header("Accept", "application/json, text/event-stream")
        .headers(
            headers -> {
              if (bearerToken != null) {
                headers.setBearerAuth(bearerToken);
              }
            })
        .bodyValue(message)
        .exchange();
  }

  /**
   * Posts a JSON-RPC request that must succeed, returning its {@code result}.
   *
   * @param method the JSON-RPC method
   * @param params the method parameters
   * @return the {@code result} member of the response
   */
  @Nonnull
  JsonNode request(@Nonnull final String method, @Nonnull final Map<String, Object> params) {
    final byte[] body =
        post(method, params).expectStatus().isOk().expectBody().returnResult().getResponseBody();
    try {
      final JsonNode response = MAPPER.readTree(body);
      if (response.has("error")) {
        throw new AssertionError("JSON-RPC error: " + response.get("error"));
      }
      return response.get("result");
    } catch (final java.io.IOException e) {
      throw new AssertionError("Unparseable response", e);
    }
  }

  /**
   * Calls a tool.
   *
   * @param name the tool name
   * @param arguments the tool arguments
   * @return the tool result, which may be an error result
   */
  @Nonnull
  ToolResult callTool(@Nonnull final String name, @Nonnull final Map<String, Object> arguments) {
    final JsonNode result = request("tools/call", Map.of("name", name, "arguments", arguments));
    return new ToolResult(
        result.path("isError").asBoolean(false),
        result.path("content").get(0).get("text").asText());
  }

  /**
   * The outcome of a tool call: whether it failed, and the text it returned.
   *
   * @param isError whether the tool reported an error
   * @param text the text content of the result
   */
  record ToolResult(boolean isError, @Nonnull String text) {

    /**
     * Parses the text as JSON.
     *
     * @return the parsed JSON
     */
    @Nonnull
    JsonNode json() {
      try {
        return MAPPER.readTree(text);
      } catch (final java.io.IOException e) {
        throw new AssertionError("Tool result is not JSON: " + text, e);
      }
    }
  }
}
