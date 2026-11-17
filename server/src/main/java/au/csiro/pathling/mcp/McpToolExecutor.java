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

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.sql.ContextArtefactParser;
import au.csiro.pathling.operations.sql.ResolvedSubject;
import au.csiro.pathling.operations.sql.SqlOperationError;
import au.csiro.pathling.operations.sql.SubjectKind;
import au.csiro.pathling.operations.sql.SubjectResolver;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.operations.sqlquery.SqlQueryPipeline;
import au.csiro.pathling.operations.view.ResultStreamingHelper;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import au.csiro.pathling.read.ReadProviderFactory;
import au.csiro.pathling.search.SearchProviderFactory;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Resource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Executes the MCP tools against the server's data, by way of the same providers and engines that
 * serve the FHIR API.
 *
 * <p>Read and search are delegated to the FHIR API's providers, so that they are subject to exactly
 * the same authority checks. The view and SQL tools are the {@code $sql-run} operation over an
 * inline subject: they require its operation authority, which the security aspect checks before
 * each method, and the engines check read access on every resource type they project.
 *
 * <p>Every method returns JSON text, which the MCP client passes to the model.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "mcp.enabled", havingValue = "true")
public class McpToolExecutor {

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final QueryableDataSource dataSource;

  @Nonnull private final ReadProviderFactory readProviderFactory;

  @Nonnull private final SearchProviderFactory searchProviderFactory;

  @Nonnull private final SubjectResolver subjectResolver;

  @Nonnull private final ContextArtefactParser contextParser;

  @Nonnull private final ViewExecutionHelper viewExecutionHelper;

  @Nonnull private final SqlQueryPipeline sqlQueryPipeline;

  @Nonnull private final Gson gson;

  @Nonnull private final ResultStreamingHelper rowConverter;

  /**
   * Constructs a new McpToolExecutor.
   *
   * @param fhirContext the FHIR context, used to parse and encode resources
   * @param dataSource the server's data source
   * @param readProviderFactory creates the FHIR API's read providers
   * @param searchProviderFactory creates the FHIR API's search providers
   * @param subjectResolver detects whether an inline artefact is a ViewDefinition or SQL Library
   * @param contextParser parses inline ViewDefinitions supplied alongside a SQL query
   * @param viewExecutionHelper the ViewDefinition evaluation engine
   * @param sqlQueryPipeline the SQL evaluation engine
   */
  @SuppressWarnings("java:S107")
  public McpToolExecutor(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final QueryableDataSource dataSource,
      @Nonnull final ReadProviderFactory readProviderFactory,
      @Nonnull final SearchProviderFactory searchProviderFactory,
      @Nonnull final SubjectResolver subjectResolver,
      @Nonnull final ContextArtefactParser contextParser,
      @Nonnull final ViewExecutionHelper viewExecutionHelper,
      @Nonnull final SqlQueryPipeline sqlQueryPipeline) {
    this.fhirContext = fhirContext;
    this.dataSource = dataSource;
    this.readProviderFactory = readProviderFactory;
    this.searchProviderFactory = searchProviderFactory;
    this.subjectResolver = subjectResolver;
    this.contextParser = contextParser;
    this.viewExecutionHelper = viewExecutionHelper;
    this.sqlQueryPipeline = sqlQueryPipeline;
    this.gson = ViewDefinitionGson.create();
    this.rowConverter = new ResultStreamingHelper(gson);
  }

  /**
   * Lists the resource types for which the server holds data.
   *
   * @return a JSON object with a sorted {@code resourceTypes} array
   */
  @Nonnull
  public String listResourceTypes() {
    final List<String> types = dataSource.getResourceTypes().stream().sorted().toList();
    return gson.toJson(Map.of("resourceTypes", types));
  }

  /**
   * Reads a resource by type and id, through the FHIR API's read provider.
   *
   * @param resourceType the resource type, e.g. {@code Patient}
   * @param id the logical id
   * @return the resource as JSON
   * @throws InvalidUserInputError if the resource type is not supported
   * @throws au.csiro.pathling.errors.ResourceNotFoundError if there is no such resource
   */
  @Nonnull
  public String readResource(@Nonnull final String resourceType, @Nonnull final String id) {
    checkSupportedType(resourceType);
    final IBaseResource resource =
        readProviderFactory.createReadProvider(resourceType).read(new IdType(resourceType, id));
    return jsonParser().encodeResourceToString(resource);
  }

  /**
   * Searches for resources of a type, through the FHIR API's search provider, returning the first
   * page of matches.
   *
   * @param resourceType the resource type, e.g. {@code Patient}
   * @param params standard FHIR search parameters as a URL query string, or null for none
   * @param filters FHIRPath expressions, all of which a resource must satisfy
   * @param count the largest number of resources to return
   * @return a {@code searchset} Bundle as JSON, whose {@code total} counts every match
   * @throws InvalidUserInputError if the resource type is not supported
   */
  @Nonnull
  public String search(
      @Nonnull final String resourceType,
      @Nullable final String params,
      @Nonnull final List<String> filters,
      final int count) {
    checkSupportedType(resourceType);
    final IBundleProvider results =
        searchProviderFactory
            .createSearchProvider(resourceType)
            .search(toFilterParam(filters), toRawParams(params));
    final int total = Objects.requireNonNull(results.size());

    final Bundle bundle = new Bundle();
    bundle.setType(BundleType.SEARCHSET);
    bundle.setTotal(total);
    if (total > 0) {
      results
          .getResources(0, Math.min(count, total))
          .forEach(resource -> bundle.addEntry().setResource((Resource) resource));
    }
    return jsonParser().encodeResourceToString(bundle);
  }

  /**
   * Runs an inline ViewDefinition over the server's data.
   *
   * @param viewDefinition the ViewDefinition as JSON
   * @param limit the largest number of rows to return
   * @return a JSON object with the {@code rows} and whether they were {@code truncated}
   */
  @Nonnull
  @OperationAccess("sql-run")
  public String runViewDefinition(@Nonnull final String viewDefinition, final int limit) {
    final ResolvedSubject subject = resolve(viewDefinition);
    if (subject.getKind() != SubjectKind.VIEW_DEFINITION) {
      throw new InvalidUserInputError(
          "The viewDefinition argument must be a ViewDefinition; use run_sql_query to run a SQL"
              + " Library.");
    }
    final FhirView view = viewExecutionHelper.parseViewDefinition(subject.getResource());
    viewExecutionHelper.checkProjectedResourceReadAuthority(view);
    return rows(viewExecutionHelper.buildQuery(view, dataSource), limit);
  }

  /**
   * Runs an inline SQLQuery or SQLView Library over the server's data.
   *
   * @param query the Library as JSON
   * @param views ViewDefinitions as JSON, which satisfy the query's dependencies in preference to
   *     those stored on the server
   * @param parameters a Parameters resource as JSON, binding the query's declared parameters, or
   *     null for none
   * @param limit the largest number of rows to return
   * @return a JSON object with the {@code rows} and whether they were {@code truncated}
   */
  @Nonnull
  @OperationAccess("sql-run")
  public String runSqlQuery(
      @Nonnull final String query,
      @Nonnull final List<String> views,
      @Nullable final String parameters,
      final int limit) {
    final ResolvedSubject subject = resolve(query);
    if (!subject.getKind().isSql()) {
      throw new InvalidUserInputError(
          "The query argument must be a SQLQuery or SQLView Library; use run_view_definition to"
              + " run a ViewDefinition.");
    }
    final SuppliedArtefacts supplied =
        contextParser.parse(views.stream().map(this::parseResource).toList());
    final PreparedSqlQuery prepared =
        sqlQueryPipeline.prepare(
            subject.asLibrary(), new IntegerType(limit + 1), parseParameters(parameters), supplied);
    supplied.checkAllMatched();

    final AtomicReference<String> output = new AtomicReference<>();
    try {
      sqlQueryPipeline.execute(prepared, dataSource, result -> output.set(rows(result, limit)));
    } catch (final Exception e) {
      // An error in the caller's SQL is reported as such, rather than as a server failure.
      @Nullable
      final UnprocessableEntityException analysisFailure =
          SqlOperationError.asAnalysisFailure(null, e);
      if (analysisFailure != null) {
        throw analysisFailure;
      }
      throw e;
    }
    return output.get();
  }

  /**
   * Collects up to {@code limit} rows, fetching one more to learn whether the result was truncated.
   */
  @Nonnull
  private String rows(@Nonnull final Dataset<Row> result, final int limit) {
    final StructType schema = result.schema();
    final List<Row> collected = result.limit(limit + 1).collectAsList();
    final List<Map<String, Object>> rows =
        collected.stream().limit(limit).map(row -> rowConverter.rowToMap(row, schema)).toList();
    final Map<String, Object> output = new LinkedHashMap<>();
    output.put("truncated", collected.size() > limit);
    output.put("rows", rows);
    return gson.toJson(output);
  }

  @Nonnull
  private ResolvedSubject resolve(@Nonnull final String json) {
    return subjectResolver.resolve(null, null, parseResource(json), null);
  }

  @Nonnull
  private IBaseResource parseResource(@Nonnull final String json) {
    return jsonParser().parseResource(json);
  }

  @Nullable
  private Parameters parseParameters(@Nullable final String json) {
    if (json == null) {
      return null;
    }
    if (parseResource(json) instanceof final Parameters parameters) {
      return parameters;
    }
    throw new InvalidUserInputError("The parameters argument must be a Parameters resource.");
  }

  @Nonnull
  private IParser jsonParser() {
    return fhirContext.newJsonParser();
  }

  /**
   * Rejects a resource type that the FHIR API does not serve, so that the model is told so rather
   * than receiving an empty or internal failure.
   */
  private static void checkSupportedType(@Nonnull final String resourceType) {
    if (FhirServer.isCustomResourceType(resourceType)) {
      return;
    }
    try {
      if (FhirServer.supportedResourceTypes().contains(ResourceType.fromCode(resourceType))) {
        return;
      }
    } catch (final FHIRException e) {
      // Not a FHIR resource type at all, which is reported below.
    }
    throw new InvalidUserInputError("Unsupported resource type: " + resourceType);
  }

  /** Turns FHIRPath expressions into the AND-combined parameter the search provider expects. */
  @Nullable
  private static StringAndListParam toFilterParam(@Nonnull final List<String> filters) {
    if (filters.isEmpty()) {
      return null;
    }
    final StringAndListParam param = new StringAndListParam();
    filters.forEach(filter -> param.addAnd(new StringOrListParam().add(new StringParam(filter))));
    return param;
  }

  /**
   * Splits a query string into the raw parameter map the search provider expects. Values are left
   * encoded, as the search provider rejoins them into a query string that is decoded once.
   */
  @Nullable
  private static Map<String, List<String>> toRawParams(@Nullable final String params) {
    if (params == null || params.isBlank()) {
      return null;
    }
    final String query = params.startsWith("?") ? params.substring(1) : params;
    final Map<String, List<String>> raw = new LinkedHashMap<>();
    for (final String pair : query.split("&")) {
      if (pair.isEmpty()) {
        continue;
      }
      final int equals = pair.indexOf('=');
      final String name = equals < 0 ? pair : pair.substring(0, equals);
      final String value = equals < 0 ? "" : pair.substring(equals + 1);
      raw.computeIfAbsent(name, key -> new ArrayList<>()).add(value);
    }
    return raw;
  }
}
