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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.view.ResultStreamingHelper;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Orchestrates the execution of the {@code $sqlquery-run} operation. Parses the SQLQuery Library
 * resource, resolves ViewDefinition dependencies, validates and executes the SQL query, and streams
 * results in the requested format.
 */
@Slf4j
@Component
public class SqlQueryExecutionHelper {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final SqlQueryLibraryParser libraryParser;

  @Nonnull private final SqlValidator sqlValidator;

  @Nonnull private final ViewRegistrationService viewRegistrationService;

  @Nonnull private final ReadExecutor readExecutor;

  @Nonnull private final Gson gson;

  @Nonnull private final ResultStreamingHelper streamingHelper;

  /**
   * Constructs a new SqlQueryExecutionHelper.
   *
   * @param sparkSession the Spark session
   * @param deltaLake the queryable data source
   * @param fhirContext the FHIR context
   * @param serverConfiguration the server configuration
   * @param libraryParser the SQLQuery Library parser
   * @param sqlValidator the SQL validator
   * @param viewRegistrationService the view registration service
   * @param readExecutor the read executor for reading stored resources
   */
  @SuppressWarnings("java:S107")
  @Autowired
  public SqlQueryExecutionHelper(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final SqlQueryLibraryParser libraryParser,
      @Nonnull final SqlValidator sqlValidator,
      @Nonnull final ViewRegistrationService viewRegistrationService,
      @Nonnull final ReadExecutor readExecutor) {
    this.sparkSession = sparkSession;
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
    this.libraryParser = libraryParser;
    this.sqlValidator = sqlValidator;
    this.viewRegistrationService = viewRegistrationService;
    this.readExecutor = readExecutor;
    this.gson = ViewDefinitionGson.create();
    this.streamingHelper = new ResultStreamingHelper(gson);
  }

  /**
   * Executes a SQL query from a Library resource and streams results to the HTTP response.
   *
   * @param libraryResource the Library resource containing the SQLQuery
   * @param format the output format, overrides Accept header if provided
   * @param acceptHeader the HTTP Accept header value, used as fallback if format is not provided
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return
   * @param parameterValues runtime parameter values to bind to the SQL query
   * @param response the HTTP response for streaming output
   */
  @SuppressWarnings("java:S107")
  public void executeSqlQuery(
      @Nonnull final IBaseResource libraryResource,
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nullable final BooleanType includeHeader,
      @Nullable final IntegerType limit,
      @Nullable final List<ParametersParameterComponent> parameterValues,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("HTTP response is required for this operation");
    }

    // Parse the Library resource as a SQLQuery.
    final Library library = castToLibrary(libraryResource);
    final ParsedSqlQuery parsedQuery = libraryParser.parse(library);

    // Resolve ViewDefinitions and check authorization.
    final Map<String, FhirView> resolvedViews = resolveViewDefinitions(parsedQuery);

    // Validate the SQL structure before execution.
    sqlValidator.validate(parsedQuery.getSql());

    // Determine output format.
    final SqlQueryOutputFormat outputFormat =
        (format != null && !format.isBlank())
            ? SqlQueryOutputFormat.fromString(format)
            : SqlQueryOutputFormat.fromAcceptHeader(acceptHeader);
    final boolean shouldIncludeHeader = includeHeader == null || includeHeader.booleanValue();

    // Register views and execute the query.
    Map<String, String> registeredViews = Map.of();
    try {
      registeredViews = viewRegistrationService.registerViews(resolvedViews, deltaLake);

      // Rewrite SQL to use prefixed temp view names.
      final String rewrittenSql =
          viewRegistrationService.rewriteSql(parsedQuery.getSql(), registeredViews);

      // Build parameter map for Spark parameterized queries.
      final Map<String, String> parameterMap = buildParameterMap(parameterValues);

      // Execute the SQL query. Use parameterized queries only when parameters are provided.
      Dataset<Row> result;
      if (parameterMap.isEmpty()) {
        result = sparkSession.sql(rewrittenSql);
      } else {
        @SuppressWarnings("unchecked")
        final java.util.Map<String, Object> objectMap =
            (java.util.Map<String, Object>) (java.util.Map<String, ?>) parameterMap;
        result = sparkSession.sql(rewrittenSql, objectMap);
      }

      // Apply limit if specified.
      if (limit != null && limit.getValue() != null) {
        result = result.limit(limit.getValue());
      }

      // Stream results.
      streamResults(result, outputFormat, shouldIncludeHeader, response);

    } finally {
      viewRegistrationService.dropViews(registeredViews.values());
    }
  }

  /** Casts a FHIR resource to a Library, throwing if it is not a Library. */
  @Nonnull
  private Library castToLibrary(@Nonnull final IBaseResource resource) {
    if (resource instanceof final Library library) {
      return library;
    }
    throw new InvalidRequestException(
        "Expected a Library resource but received: " + resource.fhirType());
  }

  /**
   * Resolves ViewDefinition references from the parsed SQL query. Each relatedArtifact reference is
   * resolved to a FhirView. Authorization is checked for each referenced resource type.
   */
  @Nonnull
  private Map<String, FhirView> resolveViewDefinitions(@Nonnull final ParsedSqlQuery parsedQuery) {
    final Map<String, FhirView> resolvedViews = new LinkedHashMap<>();

    for (final ViewArtifactReference ref : parsedQuery.getViewReferences()) {
      final String viewDefinitionId = extractViewDefinitionId(ref.getCanonicalUrl());
      final IBaseResource viewResource;
      try {
        viewResource = readExecutor.read("ViewDefinition", viewDefinitionId);
      } catch (final Exception e) {
        throw new InvalidRequestException(
            "Failed to resolve ViewDefinition for label '"
                + ref.getLabel()
                + "' with reference '"
                + ref.getCanonicalUrl()
                + "': "
                + e.getMessage());
      }

      // Parse the ViewDefinition into a FhirView.
      final FhirView view = parseViewDefinition(viewResource);

      // Check resource-level authorization for the ViewDefinition's target resource type.
      if (serverConfiguration.getAuth().isEnabled()) {
        SecurityAspect.checkHasAuthority(
            PathlingAuthority.resourceAccess(AccessType.READ, view.getResource()));
      }

      resolvedViews.put(ref.getLabel(), view);
    }

    return resolvedViews;
  }

  /**
   * Extracts a ViewDefinition ID from a canonical URL or relative reference. Supports relative
   * references like "ViewDefinition/my-id" and plain IDs.
   */
  @Nonnull
  private String extractViewDefinitionId(@Nonnull final String canonicalUrl) {
    // Handle relative references like "ViewDefinition/my-id".
    if (canonicalUrl.contains("/")) {
      final String[] parts = canonicalUrl.split("/");
      return parts[parts.length - 1];
    }
    // Plain ID.
    return canonicalUrl;
  }

  /** Parses a ViewDefinition resource into a FhirView using JSON round-tripping. */
  @Nonnull
  private FhirView parseViewDefinition(@Nonnull final IBaseResource viewResource) {
    try {
      final String viewJson = fhirContext.newJsonParser().encodeResourceToString(viewResource);
      return gson.fromJson(viewJson, FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
    }
  }

  /**
   * Builds a parameter map from FHIR Parameters components. Parameter values are converted to
   * string representations for Spark's parameterized query API.
   */
  @Nonnull
  private Map<String, String> buildParameterMap(
      @Nullable final List<ParametersParameterComponent> parameterValues) {
    final Map<String, String> params = new HashMap<>();
    if (parameterValues == null) {
      return params;
    }

    for (final ParametersParameterComponent param : parameterValues) {
      // Each parameter component has 'name' and 'value' parts.
      String name = null;
      String value = null;

      for (final ParametersParameterComponent part : param.getPart()) {
        if ("name".equals(part.getName()) && part.getValue() != null) {
          name = part.getValue().primitiveValue();
        } else if ("value".equals(part.getName()) && part.getValue() != null) {
          final Type valueType = part.getValue();
          value = valueType.primitiveValue();
        }
      }

      if (name != null && value != null) {
        params.put(name, value);
      }
    }

    return params;
  }

  /** Streams query results to the HTTP response in the specified format. */
  private void streamResults(
      @Nonnull final Dataset<Row> result,
      @Nonnull final SqlQueryOutputFormat outputFormat,
      final boolean shouldIncludeHeader,
      @Nonnull final HttpServletResponse response) {

    response.setContentType(outputFormat.getContentType());
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setStatus(HttpServletResponse.SC_OK);

    try {
      if (outputFormat == SqlQueryOutputFormat.PARQUET) {
        streamParquet(result, response);
      } else {
        streamTabular(result, outputFormat, shouldIncludeHeader, response);
      }
    } catch (final IOException e) {
      log.error("Error streaming SQL query results", e);
      throw new InvalidRequestException("Error streaming results: " + e.getMessage());
    }
  }

  /** Streams results in a tabular format (NDJSON, CSV, or JSON). */
  private void streamTabular(
      @Nonnull final Dataset<Row> result,
      @Nonnull final SqlQueryOutputFormat outputFormat,
      final boolean shouldIncludeHeader,
      @Nonnull final HttpServletResponse response)
      throws IOException {

    final OutputStream outputStream = response.getOutputStream();
    final StructType schema = result.schema();

    // For CSV with header, write header immediately to minimise TTFB.
    if (outputFormat == SqlQueryOutputFormat.CSV && shouldIncludeHeader) {
      final List<String> columnNames = java.util.Arrays.stream(schema.fieldNames()).toList();
      streamingHelper.writeCsvHeader(outputStream, columnNames);
      outputStream.flush();
    }

    final Iterator<Row> iterator = result.toLocalIterator();
    switch (outputFormat) {
      case NDJSON -> streamingHelper.streamNdjson(outputStream, iterator, schema);
      case JSON -> streamingHelper.writeJson(outputStream, iterator, schema);
      default -> streamingHelper.streamCsv(outputStream, iterator, schema);
    }
    outputStream.flush();
  }

  /** Writes results as Parquet to a temporary file and streams it to the HTTP response. */
  private void streamParquet(
      @Nonnull final Dataset<Row> result, @Nonnull final HttpServletResponse response)
      throws IOException {

    final Path tempDir = Files.createTempDirectory("sqlquery-parquet-");
    final String outputPath = tempDir.resolve("result").toString();

    try {
      result.coalesce(1).write().mode(SaveMode.Overwrite).parquet(outputPath);

      // Find the generated Parquet file.
      final Path parquetFile = findParquetFile(Path.of(outputPath));
      if (parquetFile == null) {
        throw new InvalidRequestException("Failed to generate Parquet output");
      }

      // Stream the Parquet file to the response.
      response.setContentType(SqlQueryOutputFormat.PARQUET.getContentType());
      try (final var inputStream = Files.newInputStream(parquetFile);
          final var outputStream = response.getOutputStream()) {
        inputStream.transferTo(outputStream);
        outputStream.flush();
      }
    } finally {
      // Clean up temp directory.
      deleteRecursively(tempDir);
    }
  }

  /** Finds the first Parquet file in the given directory. */
  @Nullable
  private Path findParquetFile(@Nonnull final Path directory) throws IOException {
    if (!Files.isDirectory(directory)) {
      return null;
    }
    try (final var stream = Files.list(directory)) {
      return stream.filter(p -> p.toString().endsWith(".parquet")).findFirst().orElse(null);
    }
  }

  /** Recursively deletes a directory and its contents. */
  private void deleteRecursively(@Nonnull final Path path) {
    try {
      if (Files.isDirectory(path)) {
        try (final var stream = Files.list(path)) {
          stream.forEach(this::deleteRecursively);
        }
      }
      Files.deleteIfExists(path);
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary file: {}", path, e);
    }
  }
}
