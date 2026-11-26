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

import au.csiro.pathling.config.McpConfiguration;
import au.csiro.pathling.config.OperationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.InvalidUserInputError;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.modelcontextprotocol.spec.McpSchema.ToolAnnotations;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.OperationOutcome;

/**
 * Defines the MCP tools: their names, descriptions and input schemas, and the translation of a tool
 * call into a call on the {@link McpToolExecutor}.
 *
 * <p>Every tool is read-only. A tool is offered only when the FHIR operation it corresponds to is
 * enabled. A failure is returned as a tool error carrying a message the model can act on, rather
 * than as a protocol error.
 *
 * @author John Grimes
 */
@Slf4j
public class McpTools {

  /** The number of resources the search tool returns when no count is given. */
  static final int DEFAULT_SEARCH_COUNT = 10;

  /** The number of rows the view and SQL tools return when no limit is given. */
  static final int DEFAULT_ROW_LIMIT = 100;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final ToolAnnotations READ_ONLY =
      new ToolAnnotations(null, true, false, true, false, null);

  private static final Map<String, Object> RESOURCE_TYPE =
      Map.of("type", "string", "description", "The FHIR resource type, e.g. Patient.");

  @Nonnull private final ServerConfiguration configuration;

  @Nonnull private final McpToolExecutor executor;

  /**
   * Constructs a new McpTools.
   *
   * @param configuration the server configuration, which decides the tools offered and the result
   *     size limits
   * @param executor executes the tools
   */
  public McpTools(
      @Nonnull final ServerConfiguration configuration, @Nonnull final McpToolExecutor executor) {
    this.configuration = configuration;
    this.executor = executor;
  }

  /**
   * Returns the specifications of the tools offered by this server.
   *
   * @return the tool specifications
   */
  @Nonnull
  public List<SyncToolSpecification> specifications() {
    final OperationConfiguration operations = configuration.getOperations();
    final List<SyncToolSpecification> tools = new ArrayList<>();
    tools.add(listResourceTypes());
    if (operations.isReadEnabled()) {
      tools.add(readResource());
    }
    if (operations.isSearchEnabled()) {
      tools.add(search());
    }
    if (operations.isSqlRunEnabled()) {
      tools.add(runViewDefinition());
      tools.add(runSqlQuery());
    }
    return tools;
  }

  @Nonnull
  private SyncToolSpecification listResourceTypes() {
    return tool(
        "list_resource_types",
        "List resource types",
        "List the FHIR resource types for which the server holds data. Call this first to learn"
            + " what can be read, searched and queried.",
        schema(Map.of(), List.of()),
        arguments -> executor.listResourceTypes());
  }

  @Nonnull
  private SyncToolSpecification readResource() {
    return tool(
        "read_resource",
        "Read resource",
        "Read a single FHIR resource by its type and logical id. Returns the resource as FHIR"
            + " JSON.",
        schema(
            Map.of("resourceType", RESOURCE_TYPE, "id", string("The logical id of the resource.")),
            List.of("resourceType", "id")),
        arguments ->
            executor.readResource(
                requiredString(arguments, "resourceType"), requiredString(arguments, "id")));
  }

  @Nonnull
  private SyncToolSpecification search() {
    final int max = mcp().getMaxSearchCount();
    return tool(
        "search",
        "Search resources",
        "Search for FHIR resources of one type, using standard FHIR search parameters, FHIRPath"
            + " filter expressions, or both: a resource must satisfy all of them. Returns a FHIR"
            + " searchset Bundle, in which total is the number of matching resources and entry"
            + " holds the first of them. To count, aggregate or extract fields across many"
            + " resources, use run_view_definition or run_sql_query instead.",
        schema(
            Map.of(
                "resourceType",
                RESOURCE_TYPE,
                "params",
                string(
                    "Standard FHIR search parameters as a URL query string, e.g."
                        + " gender=female&birthdate=ge1970-01-01. A comma separates alternative"
                        + " values. Do not include _count; use the count argument."),
                "filters",
                Map.of(
                    "type",
                    "array",
                    "items",
                    Map.of("type", "string"),
                    "description",
                    "FHIRPath expressions evaluated against each resource, all of which must be"
                        + " true, e.g. name.where(family = 'Smith').exists()."),
                "count",
                integer(
                    "The largest number of resources to return (default %d, at most %d)."
                        .formatted(DEFAULT_SEARCH_COUNT, max))),
            List.of("resourceType")),
        arguments ->
            executor.search(
                requiredString(arguments, "resourceType"),
                optionalString(arguments, "params"),
                stringList(arguments, "filters"),
                bounded(arguments, "count", DEFAULT_SEARCH_COUNT, max)));
  }

  @Nonnull
  private SyncToolSpecification runViewDefinition() {
    final int max = mcp().getMaxRows();
    return tool(
        "run_view_definition",
        "Run ViewDefinition",
        "Run a SQL on FHIR ViewDefinition over the server's data and return the resulting rows."
            + " A ViewDefinition flattens one resource type into a table: resource names the type,"
            + " select lists columns (each a name and a FHIRPath path) and where holds FHIRPath"
            + " filters. See https://sql-on-fhir.org/ig/StructureDefinition-ViewDefinition.html."
            + " The result is an object with rows and truncated, which is true when there were"
            + " more rows than the limit.",
        schema(
            Map.of(
                "viewDefinition",
                object(
                    "The ViewDefinition resource, e.g. {\"resourceType\": \"ViewDefinition\","
                        + " \"status\": \"active\", \"resource\": \"Patient\", \"select\":"
                        + " [{\"column\": [{\"name\": \"id\", \"path\": \"id\"}]}]}."),
                "limit",
                rowLimit(max)),
            List.of("viewDefinition")),
        arguments ->
            executor.runViewDefinition(
                json(requiredObject(arguments, "viewDefinition")),
                bounded(arguments, "limit", DEFAULT_ROW_LIMIT, max)));
  }

  @Nonnull
  private SyncToolSpecification runSqlQuery() {
    final int max = mcp().getMaxRows();
    return tool(
        "run_sql_query",
        "Run SQL query",
        "Run a SQL on FHIR SQLQuery Library over ViewDefinitions and return the resulting rows."
            + " Use this to join, filter and aggregate. Each table the SQL reads is declared in the"
            + " Library as a relatedArtifact with type depends-on, a label (the table name used in"
            + " the SQL) and a resource (the canonical URL of a ViewDefinition). Supply those"
            + " ViewDefinitions in views, each with a matching url, or reference ViewDefinitions"
            + " stored on the server. The SQL is Spark SQL. The result is an object with rows and"
            + " truncated, which is true when there were more rows than the limit.",
        schema(
            Map.of(
                "query",
                object(
                    "The SQLQuery Library: resourceType Library, status active, type coded"
                        + " sql-query from https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes,"
                        + " and a depends-on relatedArtifact for each table. Its SQL may be given"
                        + " in content as Base64 data with contentType application/sql, or in the"
                        + " sql argument."),
                "sql",
                string(
                    "The SQL as plain text. When given, it replaces any content in the Library."),
                "views",
                Map.of(
                    "type",
                    "array",
                    "items",
                    Map.of("type", "object"),
                    "description",
                    "ViewDefinition resources satisfying the Library's dependencies, each with a"
                        + " url matching a relatedArtifact resource."),
                "parameters",
                object(
                    "A FHIR Parameters resource binding values to the parameters the Library"
                        + " declares, which the SQL references as :name."),
                "limit",
                rowLimit(max)),
            List.of("query")),
        arguments ->
            executor.runSqlQuery(
                json(withSql(requiredObject(arguments, "query"), optionalString(arguments, "sql"))),
                objectList(arguments, "views").stream().map(McpTools::json).toList(),
                optionalObject(arguments, "parameters") == null
                    ? null
                    : json(optionalObject(arguments, "parameters")),
                bounded(arguments, "limit", DEFAULT_ROW_LIMIT, max)));
  }

  @Nonnull
  private McpConfiguration mcp() {
    return configuration.getMcp();
  }

  // -- Tool construction --

  @Nonnull
  private static SyncToolSpecification tool(
      @Nonnull final String name,
      @Nonnull final String title,
      @Nonnull final String description,
      @Nonnull final Map<String, Object> inputSchema,
      @Nonnull final Function<Map<String, Object>, String> body) {
    final Tool tool =
        Tool.builder(name, inputSchema)
            .title(title)
            .description(description)
            .annotations(READ_ONLY)
            .build();
    return SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (context, request) ->
                invoke(name, request.arguments() == null ? Map.of() : request.arguments(), body))
        .build();
  }

  @Nonnull
  private static CallToolResult invoke(
      @Nonnull final String name,
      @Nonnull final Map<String, Object> arguments,
      @Nonnull final Function<Map<String, Object>, String> body) {
    try {
      return result(body.apply(arguments), false);
    } catch (final Exception e) {
      return result(describe(name, e), true);
    }
  }

  @Nonnull
  private static CallToolResult result(@Nonnull final String text, final boolean isError) {
    return CallToolResult.builder()
        .content(List.of(TextContent.builder(text).build()))
        .isError(isError)
        .build();
  }

  /**
   * Describes a failure for the model, in the terms the FHIR API would have used: the diagnostics
   * of the OperationOutcome it would have returned, or its message.
   */
  @Nonnull
  private static String describe(@Nonnull final String name, @Nonnull final Exception error) {
    final BaseServerResponseException converted = ErrorHandlingInterceptor.convertError(error);
    if (converted.getStatusCode() >= 500) {
      log.error("MCP tool {} failed", name, error);
    }
    if (converted.getOperationOutcome() instanceof final OperationOutcome outcome
        && outcome.hasIssue()) {
      return outcome.getIssue().stream()
          .map(issue -> issue.hasDiagnostics() ? issue.getDiagnostics() : issue.getCode().toCode())
          .collect(Collectors.joining("; "));
    }
    return converted.getMessage();
  }

  @Nonnull
  private static Map<String, Object> schema(
      @Nonnull final Map<String, Object> properties, @Nonnull final List<String> required) {
    final Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
    schema.put("required", required);
    schema.put("additionalProperties", false);
    return schema;
  }

  @Nonnull
  private static Map<String, Object> string(@Nonnull final String description) {
    return Map.of("type", "string", "description", description);
  }

  @Nonnull
  private static Map<String, Object> integer(@Nonnull final String description) {
    return Map.of("type", "integer", "minimum", 1, "description", description);
  }

  @Nonnull
  private static Map<String, Object> object(@Nonnull final String description) {
    return Map.of("type", "object", "description", description);
  }

  @Nonnull
  private static Map<String, Object> rowLimit(final int max) {
    return integer(
        "The largest number of rows to return (default %d, at most %d)."
            .formatted(DEFAULT_ROW_LIMIT, max));
  }

  // -- Argument handling --

  @Nonnull
  private static String requiredString(
      @Nonnull final Map<String, Object> arguments, @Nonnull final String name) {
    final String value = optionalString(arguments, name);
    if (value == null || value.isBlank()) {
      throw new InvalidUserInputError("The " + name + " argument is required.");
    }
    return value;
  }

  @Nullable
  private static String optionalString(
      @Nonnull final Map<String, Object> arguments, @Nonnull final String name) {
    final Object value = arguments.get(name);
    return value == null ? null : value.toString();
  }

  @Nonnull
  private static List<String> stringList(
      @Nonnull final Map<String, Object> arguments, @Nonnull final String name) {
    return arguments.get(name) instanceof final List<?> list
        ? list.stream().map(Object::toString).toList()
        : List.of();
  }

  @Nonnull
  private static Map<String, Object> requiredObject(
      @Nonnull final Map<String, Object> arguments, @Nonnull final String name) {
    final Map<String, Object> value = optionalObject(arguments, name);
    if (value == null) {
      throw new InvalidUserInputError("The " + name + " argument must be a JSON object.");
    }
    return value;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static Map<String, Object> optionalObject(
      @Nonnull final Map<String, Object> arguments, @Nonnull final String name) {
    return arguments.get(name) instanceof final Map<?, ?> map ? (Map<String, Object>) map : null;
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> objectList(
      @Nonnull final Map<String, Object> arguments, @Nonnull final String name) {
    if (!(arguments.get(name) instanceof final List<?> list)) {
      return List.of();
    }
    return list.stream()
        .map(
            item -> {
              if (item instanceof final Map<?, ?> map) {
                return (Map<String, Object>) map;
              }
              throw new InvalidUserInputError("Each entry in " + name + " must be a JSON object.");
            })
        .toList();
  }

  /** Returns the requested number, or the default, capped at the maximum. */
  private static int bounded(
      @Nonnull final Map<String, Object> arguments,
      @Nonnull final String name,
      final int defaultValue,
      final int max) {
    final int requested =
        arguments.get(name) instanceof final Number number ? number.intValue() : defaultValue;
    return Math.max(1, Math.min(requested, max));
  }

  /** Places plain-text SQL into the Library's content, encoded as the Library requires. */
  @Nonnull
  private static Map<String, Object> withSql(
      @Nonnull final Map<String, Object> library, @Nullable final String sql) {
    if (sql == null) {
      return library;
    }
    final Map<String, Object> updated = new LinkedHashMap<>(library);
    updated.put(
        "content",
        List.of(
            Map.of(
                "contentType",
                "application/sql",
                "data",
                Base64.getEncoder().encodeToString(sql.getBytes(StandardCharsets.UTF_8)))));
    return updated;
  }

  @Nonnull
  private static String json(@Nonnull final Map<String, Object> value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (final JsonProcessingException e) {
      throw new InvalidUserInputError("Unable to serialise argument: " + e.getMessage());
    }
  }
}
