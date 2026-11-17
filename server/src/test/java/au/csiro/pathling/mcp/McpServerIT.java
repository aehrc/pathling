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

import au.csiro.pathling.mcp.McpTestClient.ToolResult;
import au.csiro.pathling.util.TestDataSetup;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Exercises the MCP endpoint through the Streamable HTTP transport, with authorisation disabled,
 * against the server's test warehouse of 125 patients (70 male, 55 female).
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
@TestPropertySource(
    properties = {
      "pathling.mcp.enabled=true",
      "pathling.mcp.maxSearchCount=20",
      "pathling.mcp.maxRows=200"
    })
class McpServerIT {

  private static final String PATIENT_ID = "72df0f76-2758-fac4-67cd-de33c4a2c95e";

  private static final String LIBRARY_TYPE_SYSTEM =
      "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes";

  private static final String PATIENT_VIEW_URL = "https://example.org/ViewDefinition/patients";

  @TempDir private static Path warehouseDir;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  McpTestClient mcp;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setUp() {
    mcp = new McpTestClient(webTestClient, port, null);
  }

  // -- Protocol --

  @Test
  void initializeIdentifiesTheServerAndOffersTools() {
    final JsonNode result =
        mcp.request(
            "initialize",
            Map.of(
                "protocolVersion",
                "2025-06-18",
                "capabilities",
                Map.of(),
                "clientInfo",
                Map.of("name", "test", "version", "1.0")));

    assertThat(result.path("serverInfo").path("name").asText()).isEqualTo("Pathling");
    assertThat(result.path("capabilities").has("tools")).isTrue();
    assertThat(result.path("instructions").asText()).isNotBlank();
  }

  @Test
  void listsTheReadOnlyTools() {
    final JsonNode tools = mcp.request("tools/list", Map.of()).path("tools");

    final List<String> names =
        StreamSupport.stream(tools.spliterator(), false).map(t -> t.path("name").asText()).toList();
    assertThat(names)
        .containsExactlyInAnyOrder(
            "list_resource_types",
            "read_resource",
            "search",
            "run_view_definition",
            "run_sql_query");
    // Every tool is advertised as read-only, so that clients need not seek confirmation for it.
    assertThat(tools)
        .allSatisfy(
            tool -> {
              assertThat(tool.path("description").asText()).isNotBlank();
              assertThat(tool.path("annotations").path("readOnlyHint").asBoolean()).isTrue();
            });
  }

  // -- list_resource_types --

  @Test
  void listResourceTypesReportsTheTypesHoldingData() {
    final ToolResult result = mcp.callTool("list_resource_types", Map.of());

    assertThat(result.isError()).isFalse();
    final List<String> types =
        StreamSupport.stream(result.json().path("resourceTypes").spliterator(), false)
            .map(JsonNode::asText)
            .toList();
    assertThat(types).contains("Patient", "Observation", "Condition").isSorted();
  }

  // -- read_resource --

  @Test
  void readResourceReturnsTheResource() {
    final ToolResult result =
        mcp.callTool("read_resource", Map.of("resourceType", "Patient", "id", PATIENT_ID));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("resourceType").asText()).isEqualTo("Patient");
    assertThat(result.json().path("id").asText()).isEqualTo(PATIENT_ID);
  }

  @Test
  void readResourceReportsAMissingResourceAsAToolError() {
    final ToolResult result =
        mcp.callTool("read_resource", Map.of("resourceType", "Patient", "id", "does-not-exist"));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("not found");
  }

  @Test
  void readResourceRejectsAnUnknownResourceType() {
    final ToolResult result =
        mcp.callTool("read_resource", Map.of("resourceType", "Spaceship", "id", "1"));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("Spaceship");
  }

  @Test
  void readResourceRequiresItsArguments() {
    // The input schema marks both arguments as required, so the call is refused before execution.
    final ToolResult result = mcp.callTool("read_resource", Map.of("resourceType", "Patient"));

    assertThat(result.isError()).isTrue();
  }

  // -- search --

  @Test
  void searchAppliesStandardSearchParameters() {
    final ToolResult result =
        mcp.callTool(
            "search", Map.of("resourceType", "Patient", "params", "gender=male", "count", 5));

    assertThat(result.isError()).isFalse();
    final JsonNode bundle = result.json();
    assertThat(bundle.path("resourceType").asText()).isEqualTo("Bundle");
    assertThat(bundle.path("type").asText()).isEqualTo("searchset");
    assertThat(bundle.path("total").asInt()).isEqualTo(70);
    assertThat(bundle.path("entry")).hasSize(5);
    assertThat(bundle.path("entry"))
        .allSatisfy(
            entry -> assertThat(entry.path("resource").path("gender").asText()).isEqualTo("male"));
  }

  @Test
  void searchAppliesFhirPathFilters() {
    final ToolResult result =
        mcp.callTool(
            "search",
            Map.of("resourceType", "Patient", "filters", List.of("gender = 'female'"), "count", 3));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("total").asInt()).isEqualTo(55);
  }

  @Test
  void searchCombinesParametersAndFiltersWithAnd() {
    // No patient is both male and female, so the conjunction matches nothing.
    final ToolResult result =
        mcp.callTool(
            "search",
            Map.of(
                "resourceType",
                "Patient",
                "params",
                "gender=male",
                "filters",
                List.of("gender = 'female'")));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("total").asInt()).isZero();
    assertThat(result.json().path("entry").isMissingNode()).isTrue();
  }

  @Test
  void searchDecodesPercentEncodedParameterValues() {
    // A value may arrive percent-encoded, as it would in a URL. The encoded comma separates two
    // alternatives, so every patient matches; left encoded, it would match none.
    final ToolResult result =
        mcp.callTool("search", Map.of("resourceType", "Patient", "params", "gender=male%2Cfemale"));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("total").asInt()).isEqualTo(125);
  }

  @Test
  void searchReturnsTenResourcesByDefault() {
    final ToolResult result = mcp.callTool("search", Map.of("resourceType", "Patient"));

    assertThat(result.json().path("total").asInt()).isEqualTo(125);
    assertThat(result.json().path("entry")).hasSize(10);
  }

  @Test
  void searchCapsTheCountAtTheConfiguredMaximum() {
    final ToolResult result =
        mcp.callTool("search", Map.of("resourceType", "Patient", "count", 50));

    assertThat(result.json().path("entry")).hasSize(20);
  }

  @Test
  void searchReportsAnInvalidFilterAsAToolError() {
    final ToolResult result =
        mcp.callTool(
            "search", Map.of("resourceType", "Patient", "filters", List.of("gender = = 'x'")));

    assertThat(result.isError()).isTrue();
  }

  // -- run_view_definition --

  @Test
  void runViewDefinitionReturnsRowsAndReportsTruncation() {
    final ToolResult result =
        mcp.callTool(
            "run_view_definition", Map.of("viewDefinition", patientView(null), "limit", 5));

    assertThat(result.isError()).isFalse();
    final JsonNode json = result.json();
    assertThat(json.path("truncated").asBoolean()).isTrue();
    assertThat(json.path("rows")).hasSize(5);
    assertThat(json.path("rows").get(0).has("id")).isTrue();
    assertThat(json.path("rows").get(0).has("gender")).isTrue();
  }

  @Test
  void runViewDefinitionReportsACompleteResultAsNotTruncated() {
    final ToolResult result =
        mcp.callTool(
            "run_view_definition", Map.of("viewDefinition", patientView(null), "limit", 150));

    assertThat(result.json().path("truncated").asBoolean()).isFalse();
    assertThat(result.json().path("rows")).hasSize(125);
  }

  @Test
  void runViewDefinitionReturnsOneHundredRowsByDefault() {
    final ToolResult result =
        mcp.callTool("run_view_definition", Map.of("viewDefinition", patientView(null)));

    assertThat(result.json().path("rows")).hasSize(100);
    assertThat(result.json().path("truncated").asBoolean()).isTrue();
  }

  @Test
  void runViewDefinitionCapsTheLimitAtTheConfiguredMaximum() {
    final ToolResult result =
        mcp.callTool(
            "run_view_definition", Map.of("viewDefinition", observationView(), "limit", 500));

    assertThat(result.json().path("rows")).hasSize(200);
    assertThat(result.json().path("truncated").asBoolean()).isTrue();
  }

  @Test
  void runViewDefinitionAppliesTheViewsWhereClause() {
    final ToolResult result =
        mcp.callTool(
            "run_view_definition",
            Map.of("viewDefinition", patientView("gender = 'female'"), "limit", 200));

    assertThat(result.json().path("rows")).hasSize(55);
  }

  @Test
  void runViewDefinitionReportsAnInvalidViewAsAToolError() {
    final Map<String, Object> view = patientView(null);
    view.remove("resource");

    final ToolResult result = mcp.callTool("run_view_definition", Map.of("viewDefinition", view));

    assertThat(result.isError()).isTrue();
  }

  @Test
  void runViewDefinitionRejectsASqlQuery() {
    final ToolResult result =
        mcp.callTool(
            "run_view_definition",
            Map.of("viewDefinition", sqlQuery("SELECT id FROM pat", List.of())));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("ViewDefinition");
  }

  // -- run_sql_query --

  @Test
  void runSqlQueryOverAnInlineView() {
    final ToolResult result =
        mcp.callTool(
            "run_sql_query",
            Map.of(
                "query",
                sqlQuery(
                    "SELECT gender, COUNT(*) AS n FROM pat GROUP BY gender ORDER BY gender",
                    List.of()),
                "views",
                List.of(patientView(null))));

    assertThat(result.isError()).isFalse();
    final JsonNode rows = result.json().path("rows");
    assertThat(result.json().path("truncated").asBoolean()).isFalse();
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).path("gender").asText()).isEqualTo("female");
    assertThat(rows.get(0).path("n").asInt()).isEqualTo(55);
    assertThat(rows.get(1).path("gender").asText()).isEqualTo("male");
    assertThat(rows.get(1).path("n").asInt()).isEqualTo(70);
  }

  @Test
  void runSqlQueryAcceptsPlainTextSql() {
    // The SQL may be given as plain text rather than as Base64 data within the Library, which
    // models are prone to encode incorrectly.
    final Map<String, Object> library = sqlQuery("SELECT 1 AS ignored FROM pat", List.of());
    library.remove("content");

    final ToolResult result =
        mcp.callTool(
            "run_sql_query",
            Map.of(
                "query",
                library,
                "sql",
                "SELECT COUNT(*) AS n FROM pat WHERE gender = 'male'",
                "views",
                List.of(patientView(null))));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("rows").get(0).path("n").asInt()).isEqualTo(70);
  }

  @Test
  void runSqlQueryPrefersPlainTextSqlOverLibraryContent() {
    final ToolResult result =
        mcp.callTool(
            "run_sql_query",
            Map.of(
                "query",
                sqlQuery("SELECT no_such_column FROM pat", List.of()),
                "sql",
                "SELECT COUNT(*) AS n FROM pat",
                "views",
                List.of(patientView(null))));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("rows").get(0).path("n").asInt()).isEqualTo(125);
  }

  @Test
  void runSqlQueryBindsDeclaredParameters() {
    final Map<String, Object> parameters =
        Map.of(
            "resourceType",
            "Parameters",
            "parameter",
            List.of(Map.of("name", "g", "valueString", "female")));

    final ToolResult result =
        mcp.callTool(
            "run_sql_query",
            Map.of(
                "query",
                sqlQuery(
                    "SELECT COUNT(*) AS n FROM pat WHERE gender = :g",
                    List.of(Map.of("name", "g", "use", "in", "type", "string"))),
                "views",
                List.of(patientView(null)),
                "parameters",
                parameters));

    assertThat(result.isError()).isFalse();
    assertThat(result.json().path("rows").get(0).path("n").asInt()).isEqualTo(55);
  }

  @Test
  void runSqlQueryReportsTruncation() {
    final ToolResult result =
        mcp.callTool(
            "run_sql_query",
            Map.of(
                "query",
                sqlQuery("SELECT id FROM pat", List.of()),
                "views",
                List.of(patientView(null)),
                "limit",
                3));

    assertThat(result.json().path("rows")).hasSize(3);
    assertThat(result.json().path("truncated").asBoolean()).isTrue();
  }

  @Test
  void runSqlQueryReportsInvalidSqlAsAToolError() {
    final ToolResult result =
        mcp.callTool(
            "run_sql_query",
            Map.of(
                "query",
                sqlQuery("SELECT no_such_column FROM pat", List.of()),
                "views",
                List.of(patientView(null))));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("no_such_column");
  }

  @Test
  void runSqlQueryReportsAnUnresolvableViewAsAToolError() {
    // The query depends on a view that is neither stored nor supplied.
    final ToolResult result =
        mcp.callTool("run_sql_query", Map.of("query", sqlQuery("SELECT id FROM pat", List.of())));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains(PATIENT_VIEW_URL);
  }

  @Test
  void runSqlQueryRejectsAViewDefinition() {
    final ToolResult result = mcp.callTool("run_sql_query", Map.of("query", patientView(null)));

    assertThat(result.isError()).isTrue();
    assertThat(result.text()).contains("SQLQuery");
  }

  // -- Helpers --

  /** A ViewDefinition over Patient projecting id and gender, optionally filtered. */
  private static Map<String, Object> patientView(final String where) {
    final Map<String, Object> view = new LinkedHashMap<>();
    view.put("resourceType", "ViewDefinition");
    view.put("url", PATIENT_VIEW_URL);
    view.put("name", "patients");
    view.put("status", "active");
    view.put("resource", "Patient");
    view.put(
        "select",
        List.of(
            Map.of(
                "column",
                List.of(
                    Map.of("name", "id", "path", "id"),
                    Map.of("name", "gender", "path", "gender")))));
    if (where != null) {
      view.put("where", List.of(Map.of("path", where)));
    }
    return view;
  }

  /** A ViewDefinition over Observation projecting id, which has far more rows than any cap. */
  private static Map<String, Object> observationView() {
    return Map.of(
        "resourceType",
        "ViewDefinition",
        "name",
        "observations",
        "status",
        "active",
        "resource",
        "Observation",
        "select",
        List.of(Map.of("column", List.of(Map.of("name", "id", "path", "id")))));
  }

  /** A SQLQuery Library reading the patient view under the label {@code pat}. */
  private static Map<String, Object> sqlQuery(
      final String sql, final List<Map<String, Object>> parameters) {
    final Map<String, Object> library = new LinkedHashMap<>();
    library.put("resourceType", "Library");
    library.put("status", "active");
    library.put(
        "type",
        Map.of("coding", List.of(Map.of("system", LIBRARY_TYPE_SYSTEM, "code", "sql-query"))));
    library.put(
        "relatedArtifact",
        List.of(Map.of("type", "depends-on", "label", "pat", "resource", PATIENT_VIEW_URL)));
    if (!parameters.isEmpty()) {
      library.put("parameter", new ArrayList<>(parameters));
    }
    library.put(
        "content",
        List.of(
            Map.of(
                "contentType",
                "application/sql",
                "data",
                java.util.Base64.getEncoder()
                    .encodeToString(sql.getBytes(java.nio.charset.StandardCharsets.UTF_8)))));
    return library;
  }
}
