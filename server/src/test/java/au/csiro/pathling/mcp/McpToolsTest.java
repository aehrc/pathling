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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.OperationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link McpTools}: which tools are offered, how arguments reach the executor, and how
 * failures become tool errors rather than protocol errors.
 *
 * @author John Grimes
 */
class McpToolsTest {

  private ServerConfiguration configuration;

  private McpToolExecutor executor;

  @BeforeEach
  void setUp() {
    configuration = new ServerConfiguration();
    configuration.setOperations(new OperationConfiguration());
    executor = mock(McpToolExecutor.class);
  }

  // -- Tool selection --

  @Test
  void offersEveryToolWhenAllOperationsAreEnabled() {
    assertThat(toolNames())
        .containsExactly(
            "list_resource_types",
            "read_resource",
            "search",
            "run_view_definition",
            "run_sql_query");
  }

  @Test
  void omitsReadWhenReadIsDisabled() {
    configuration.getOperations().setReadEnabled(false);

    assertThat(toolNames()).doesNotContain("read_resource").contains("search");
  }

  @Test
  void omitsSearchWhenSearchIsDisabled() {
    configuration.getOperations().setSearchEnabled(false);

    assertThat(toolNames()).doesNotContain("search").contains("read_resource");
  }

  @Test
  void omitsTheViewAndSqlToolsWhenSqlRunIsDisabled() {
    // Both tools are the $sql-run operation under another name, and are governed by its switch.
    configuration.getOperations().setSqlRunEnabled(false);

    assertThat(toolNames())
        .doesNotContain("run_view_definition", "run_sql_query")
        .contains("list_resource_types");
  }

  // -- Argument handling --

  @Test
  void searchPassesArgumentsWithDefaultCount() {
    when(executor.search(anyString(), any(), anyList(), anyInt())).thenReturn("{}");

    call("search", Map.of("resourceType", "Patient", "params", "gender=male"));

    verify(executor).search("Patient", "gender=male", List.of(), 10);
  }

  @Test
  void searchCapsCountAtTheConfiguredMaximum() {
    configuration.getMcp().setMaxSearchCount(25);
    when(executor.search(anyString(), any(), anyList(), anyInt())).thenReturn("{}");

    call("search", Map.of("resourceType", "Patient", "count", 1000));

    verify(executor).search(eq("Patient"), isNull(), eq(List.of()), eq(25));
  }

  @Test
  void viewLimitDefaultsToOneHundredAndIsCappedAtMaxRows() {
    configuration.getMcp().setMaxRows(50);
    when(executor.runViewDefinition(anyString(), anyInt())).thenReturn("{}");

    call("run_view_definition", Map.of("viewDefinition", Map.of("resourceType", "ViewDefinition")));

    // The default of 100 exceeds the configured maximum, so the maximum applies.
    verify(executor).runViewDefinition(anyString(), eq(50));
  }

  // -- Errors --

  @Test
  void executorFailureBecomesAToolError() {
    // An access failure is reported to the model as a tool error it can read, not as a protocol
    // error that the client may hide.
    when(executor.readResource("Patient", "1")).thenThrow(new AccessDeniedError("Missing it"));

    final CallToolResult result =
        call("read_resource", Map.of("resourceType", "Patient", "id", "1"));

    assertThat(result.isError()).isTrue();
    assertThat(((TextContent) result.content().get(0)).text()).contains("Missing it");
  }

  @Test
  void successfulResultIsReturnedAsText() {
    when(executor.listResourceTypes()).thenReturn("{\"resourceTypes\":[]}");

    final CallToolResult result = call("list_resource_types", Map.of());

    assertThat(result.isError()).isFalse();
    assertThat(((TextContent) result.content().get(0)).text()).isEqualTo("{\"resourceTypes\":[]}");
  }

  private List<String> toolNames() {
    return new McpTools(configuration, executor)
        .specifications().stream().map(spec -> spec.tool().name()).toList();
  }

  private CallToolResult call(final String name, final Map<String, Object> arguments) {
    final SyncToolSpecification spec =
        new McpTools(configuration, executor)
            .specifications().stream()
                .filter(s -> s.tool().name().equals(name))
                .findFirst()
                .orElseThrow();
    return spec.callHandler().apply(null, new CallToolRequest(name, arguments, null));
  }
}
