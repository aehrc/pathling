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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.security.OperationAccess;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewBuilder;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provides an extended ViewDefinition run operation that executes multiple ViewDefinitions and
 * joins their results across resource types.
 *
 * <p>This extends the standard {@code $viewdefinition-run} operation by accepting multiple view
 * definitions with cross-resource join semantics. Each view defines a query over a single resource
 * type, with optional join information to link results back to an anchor type. The operation
 * executes each view, performs joins based on the specified join columns, and streams the resulting
 * tabular data in the same format as {@code $viewdefinition-run} (NDJSON, CSV, or JSON).
 *
 * <p>The operation accepts the following parameters:
 *
 * <ul>
 *   <li>{@code anchorType} (code) - The resource type to return (e.g., "Patient").
 *   <li>{@code view} (repeating part) - One or more view definitions, each containing:
 *       <ul>
 *         <li>{@code resource} (code) - The FHIR resource type for this view.
 *         <li>{@code select} (string, repeating) - FHIRPath expressions for columns to select.
 *         <li>{@code where} (string, repeating) - FHIRPath filter expressions (AND logic).
 *         <li>{@code joinTo} (code, optional) - Resource type to join to.
 *         <li>{@code joinOn} (string, optional) - FHIRPath expression for the join column.
 *       </ul>
 *   <li>{@code _limit} (integer) - Maximum results to return (default: 100).
 *   <li>{@code _format} (string) - Output format: "ndjson", "csv", or "json".
 *   <li>{@code header} (boolean) - Whether to include a header row in CSV output (default: true).
 * </ul>
 *
 * @author jkiddo
 * @see ViewDefinitionRunProvider
 */
@Slf4j
@Component
public class ExtendedViewDefinitionRunProvider {

  private static final int DEFAULT_MAX_RESULTS = 100;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final ViewExecutionHelper viewExecutionHelper;

  @Nonnull private final ServerConfiguration serverConfiguration;

  /**
   * Constructs a new ExtendedViewDefinitionRunProvider.
   *
   * @param deltaLake the queryable data source
   * @param viewExecutionHelper the helper for streaming view results
   * @param serverConfiguration the server configuration
   */
  @Autowired
  public ExtendedViewDefinitionRunProvider(
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final ViewExecutionHelper viewExecutionHelper,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.deltaLake = deltaLake;
    this.viewExecutionHelper = viewExecutionHelper;
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Executes an extended ViewDefinition run based on client-provided view definitions.
   *
   * <p>The operation parses the input parameters, executes each view, joins the results based on
   * the specified join columns, and streams the resulting tabular data in the requested format.
   *
   * @param parameters the Parameters resource containing the view definitions and configuration
   * @param requestDetails the servlet request details containing HTTP headers
   * @param response the HTTP response for streaming output
   */
  @Operation(name = "$extended-viewdefinition-run", idempotent = true, manualResponse = true)
  @OperationAccess("view-run")
  public void run(
      @Nonnull final Parameters parameters,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {

    if (response == null) {
      throw new InvalidRequestException("HTTP response is required for this operation");
    }

    if (parameters.getParameter().isEmpty()) {
      throw new InvalidRequestException("Parameters must include anchorType and at least one view");
    }

    final String anchorType = extractAnchorType(parameters);
    final List<ExtendedViewSpec> views = extractViews(parameters);
    final IntegerType limit = extractLimit(parameters);
    final String format = extractFormat(parameters);
    final BooleanType includeHeader = extractHeader(parameters);

    if (views.isEmpty()) {
      throw new InvalidRequestException("At least one view must be specified");
    }

    // Check resource-level authorization for all resource types involved.
    if (serverConfiguration.getAuth().isEnabled()) {
      for (final ExtendedViewSpec view : views) {
        SecurityAspect.checkHasAuthority(
            PathlingAuthority.resourceAccess(AccessType.READ, view.getResourceType()));
      }
    }

    log.info("Received {} view(s) for anchorType={}", views.size(), anchorType);

    // Find and execute the anchor view.
    final ExtendedViewSpec anchorView =
        views.stream()
            .filter(v -> v.getResourceType().equalsIgnoreCase(anchorType))
            .findFirst()
            .orElseThrow(
                () ->
                    new InvalidRequestException("No view defined for anchor type: " + anchorType));

    Dataset<Row> resultDf = executeView(anchorView, "id");
    log.info("Anchor view '{}' produced {} rows", anchorType, resultDf.count());

    // Execute join views and intersect results with the anchor IDs.
    final List<ExtendedViewSpec> joinViews =
        views.stream()
            .filter(
                v ->
                    anchorType.equalsIgnoreCase(v.getJoinTo())
                        && !v.getResourceType().equalsIgnoreCase(anchorType))
            .toList();

    for (final ExtendedViewSpec joinView : joinViews) {
      final String joinColumnName = joinView.getJoinOn();
      if (joinColumnName == null) {
        throw new InvalidRequestException(
            "View for " + joinView.getResourceType() + " must specify joinOn when joinTo is set");
      }
      final String sanitizedJoinColumn = sanitizeColumnName(joinColumnName);

      final Dataset<Row> joinDf = executeView(joinView, joinColumnName);

      // Extract IDs from the join column by stripping the resource type prefix.
      final Dataset<Row> joinIdsDf =
          joinDf.selectExpr(
              "regexp_replace(" + sanitizedJoinColumn + ", '" + anchorType + "/', '') as id");

      log.info(
          "Join view '{}' (joinTo={}, joinOn={}) produced {} rows, {} IDs",
          joinView.getResourceType(),
          joinView.getJoinTo(),
          joinColumnName,
          joinDf.count(),
          joinIdsDf.count());

      resultDf = resultDf.join(joinIdsDf, "id");
    }

    // Deduplicate the results.
    resultDf = resultDf.distinct();
    log.info("Final result after joins: {} rows", resultDf.count());

    // Stream the results using the same format as $viewdefinition-run.
    final String acceptHeader = requestDetails.getServletRequest().getHeader("Accept");
    viewExecutionHelper.streamDataset(
        resultDf, format, acceptHeader, includeHeader, limit, response);
  }

  /**
   * Executes a single view specification and returns a DataFrame with the selected columns.
   *
   * @param viewSpec the view specification to execute
   * @param primaryColumn the column name that must be present in the output (e.g., "id" or a join
   *     column)
   * @return a Dataset containing the view results
   */
  @Nonnull
  private Dataset<Row> executeView(
      @Nonnull final ExtendedViewSpec viewSpec, @Nonnull final String primaryColumn) {
    final FhirViewBuilder viewBuilder = FhirView.ofResource(viewSpec.getResourceType());

    // Ensure the primary column is included even if not explicitly selected.
    final List<String> selectExpressions = new ArrayList<>(viewSpec.getSelectExpressions());
    final String sanitizedPrimary = sanitizeColumnName(primaryColumn);
    final boolean hasPrimary =
        selectExpressions.stream()
            .anyMatch(
                expr ->
                    expr.equals(primaryColumn)
                        || sanitizeColumnName(expr).equals(sanitizedPrimary));
    if (!hasPrimary) {
      selectExpressions.add(primaryColumn);
    }

    // Build column definitions from the select expressions.
    final Column[] columns =
        selectExpressions.stream()
            .map(expr -> FhirView.column(sanitizeColumnName(expr), expr))
            .toArray(Column[]::new);

    if (columns.length > 0) {
      viewBuilder.select(FhirView.columns(columns));
    } else {
      viewBuilder.select(FhirView.columns(FhirView.column("id", "id")));
    }

    // Add where filters.
    viewSpec.getWhereExpressions().forEach(viewBuilder::where);

    final FhirView view = viewBuilder.build();
    return deltaLake.view(view).execute();
  }

  /**
   * Sanitizes a FHIRPath expression to produce a valid SQL column name.
   *
   * @param expression the FHIRPath expression to sanitize
   * @return a SQL-safe column name
   */
  @Nonnull
  private static String sanitizeColumnName(@Nonnull final String expression) {
    return expression.replace(".", "_").replace("(", "").replace(")", "");
  }

  /**
   * Extracts the anchor type from the parameters.
   *
   * @param parameters the input parameters
   * @return the anchor resource type code
   */
  @Nonnull
  private static String extractAnchorType(@Nonnull final Parameters parameters) {
    return parameters.getParameter().stream()
        .filter(p -> "anchorType".equals(p.getName()))
        .findFirst()
        .map(p -> ((CodeType) p.getValue()).getValue())
        .orElseThrow(() -> new InvalidRequestException("anchorType parameter is required"));
  }

  /**
   * Extracts and parses view definitions from the parameters.
   *
   * @param parameters the input parameters
   * @return a list of parsed view specifications
   */
  @Nonnull
  private static List<ExtendedViewSpec> extractViews(@Nonnull final Parameters parameters) {
    return parameters.getParameter().stream()
        .filter(p -> "view".equals(p.getName()))
        .map(ExtendedViewDefinitionRunProvider::parseViewSpec)
        .toList();
  }

  /**
   * Parses a single view parameter component into an ExtendedViewSpec.
   *
   * @param viewParam the view parameter component containing the view definition parts
   * @return the parsed view specification
   */
  @Nonnull
  private static ExtendedViewSpec parseViewSpec(
      @Nonnull final ParametersParameterComponent viewParam) {
    String resourceType = null;
    final List<String> selectExpressions = new ArrayList<>();
    final List<String> whereExpressions = new ArrayList<>();
    String joinTo = null;
    String joinOn = null;

    for (final ParametersParameterComponent part : viewParam.getPart()) {
      switch (part.getName()) {
        case "resource" -> resourceType = ((CodeType) part.getValue()).getValue();
        case "select" -> selectExpressions.add(((StringType) part.getValue()).getValue());
        case "where" -> whereExpressions.add(((StringType) part.getValue()).getValue());
        case "joinTo" -> joinTo = ((CodeType) part.getValue()).getValue().trim();
        case "joinOn" -> joinOn = ((StringType) part.getValue()).getValue().trim();
        default -> log.warn("Unknown view parameter part: {}", part.getName());
      }
    }

    if (resourceType == null) {
      throw new InvalidRequestException("Each view must specify a 'resource' type");
    }

    return new ExtendedViewSpec(resourceType, selectExpressions, whereExpressions, joinTo, joinOn);
  }

  /**
   * Extracts the maximum result limit from the parameters.
   *
   * @param parameters the input parameters
   * @return the limit as an IntegerType, or a default value
   */
  @Nonnull
  private static IntegerType extractLimit(@Nonnull final Parameters parameters) {
    return parameters.getParameter().stream()
        .filter(p -> "_limit".equals(p.getName()))
        .findFirst()
        .map(p -> (IntegerType) p.getValue())
        .orElse(new IntegerType(DEFAULT_MAX_RESULTS));
  }

  /**
   * Extracts the output format from the parameters.
   *
   * @param parameters the input parameters
   * @return the format string, or null if not specified
   */
  @Nullable
  private static String extractFormat(@Nonnull final Parameters parameters) {
    return parameters.getParameter().stream()
        .filter(p -> "_format".equals(p.getName()))
        .findFirst()
        .map(p -> ((StringType) p.getValue()).getValue())
        .orElse(null);
  }

  /**
   * Extracts the CSV header inclusion flag from the parameters.
   *
   * @param parameters the input parameters
   * @return the header flag, or null if not specified (defaults to true in the streaming logic)
   */
  @Nullable
  private static BooleanType extractHeader(@Nonnull final Parameters parameters) {
    return parameters.getParameter().stream()
        .filter(p -> "header".equals(p.getName()))
        .findFirst()
        .map(p -> (BooleanType) p.getValue())
        .orElse(null);
  }
}
