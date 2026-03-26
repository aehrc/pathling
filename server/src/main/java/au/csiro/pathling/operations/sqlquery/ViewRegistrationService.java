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

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Manages the lifecycle of Spark temporary views for SQL query execution. Each execution registers
 * ViewDefinition results as temporary views with UUID-prefixed names for thread safety, and drops
 * them after execution completes.
 */
@Slf4j
@Component
public class ViewRegistrationService {

  private static final String VIEW_NAME_PREFIX = "sqlquery_";

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final QueryConfiguration queryConfiguration;

  /**
   * Constructs a new ViewRegistrationService.
   *
   * @param sparkSession the Spark session
   * @param fhirContext the FHIR context
   * @param serverConfiguration the server configuration
   */
  @Autowired
  public ViewRegistrationService(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.sparkSession = sparkSession;
    this.fhirContext = fhirContext;
    this.queryConfiguration = serverConfiguration.getQuery();
  }

  /**
   * Registers resolved ViewDefinitions as Spark temporary views. Each view is given a UUID-prefixed
   * name to avoid conflicts between concurrent executions.
   *
   * @param resolvedViews a map from label (table alias) to the parsed FhirView
   * @param dataSource the data source to use for view execution
   * @return a map from original label to the actual temporary view name
   */
  @Nonnull
  public Map<String, String> registerViews(
      @Nonnull final Map<String, FhirView> resolvedViews, @Nonnull final DataSource dataSource) {

    final String executionId = UUID.randomUUID().toString().replace("-", "");
    final Map<String, String> labelToViewName = new LinkedHashMap<>();

    for (final Map.Entry<String, FhirView> entry : resolvedViews.entrySet()) {
      final String label = entry.getKey();
      final FhirView view = entry.getValue();
      final String tempViewName = VIEW_NAME_PREFIX + executionId + "_" + label;

      final FhirViewExecutor executor =
          new FhirViewExecutor(fhirContext, dataSource, queryConfiguration);
      final Dataset<Row> result;
      try {
        result = executor.buildQuery(view);
      } catch (final Exception e) {
        // Drop any views that were already registered before failing.
        dropViews(labelToViewName.values());
        throw new InvalidRequestException(
            "Failed to execute ViewDefinition for label '" + label + "': " + e.getMessage());
      }

      result.createOrReplaceTempView(tempViewName);
      labelToViewName.put(label, tempViewName);
      log.debug("Registered temporary view '{}' for label '{}'", tempViewName, label);
    }

    return labelToViewName;
  }

  /**
   * Drops the specified temporary views from the Spark session.
   *
   * @param tempViewNames the names of the temporary views to drop
   */
  public void dropViews(@Nonnull final Collection<String> tempViewNames) {
    for (final String viewName : tempViewNames) {
      try {
        sparkSession.catalog().dropTempView(viewName);
        log.debug("Dropped temporary view '{}'", viewName);
      } catch (final Exception e) {
        log.warn("Failed to drop temporary view '{}': {}", viewName, e.getMessage());
      }
    }
  }

  /**
   * Rewrites SQL table references to use the prefixed temporary view names. Each label in the SQL
   * is replaced with the corresponding temporary view name.
   *
   * @param sql the original SQL query
   * @param labelToViewName the mapping from labels to temporary view names
   * @return the rewritten SQL query
   */
  @Nonnull
  public String rewriteSql(
      @Nonnull final String sql, @Nonnull final Map<String, String> labelToViewName) {

    String rewritten = sql;
    // Replace labels in order of longest first to avoid partial replacements.
    final var sortedEntries =
        labelToViewName.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()))
            .toList();

    for (final Map.Entry<String, String> entry : sortedEntries) {
      final String label = entry.getKey();
      final String viewName = entry.getValue();
      // Replace word-boundary-delimited occurrences of the label. This handles the label as a
      // table reference in FROM, JOIN, and subquery contexts.
      final Pattern pattern = Pattern.compile("\\b" + Pattern.quote(label) + "\\b");
      final Matcher matcher = pattern.matcher(rewritten);
      rewritten = matcher.replaceAll(viewName);
    }

    return rewritten;
  }
}
