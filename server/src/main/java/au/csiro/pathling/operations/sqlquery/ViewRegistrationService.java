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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Manages the lifecycle of Spark temporary views for SQL query execution. Each execution scopes its
 * views to the HAPI per-request id so that concurrent {@code $sqlquery-run} requests using the same
 * label cannot clobber one another in Spark's session-global temporary view catalog.
 */
@Slf4j
@Component
public class ViewRegistrationService {

  private static final String VIEW_NAME_PREFIX = "sqlquery_";

  private static final Pattern UNSAFE_REQUEST_ID_CHARS = Pattern.compile("[^A-Za-z0-9_]");

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
   * Registers resolved ViewDefinitions as Spark temporary views, scoped by the request id so that
   * concurrent executions cannot clobber one another in the session-global catalog.
   *
   * @param resolvedViews a map from label (table alias) to the parsed FhirView
   * @param dataSource the data source to use for view execution
   * @param requestId the per-request id used to namespace registered view names
   * @return a map from original label to the actual temporary view name
   */
  @Nonnull
  public Map<String, String> registerViews(
      @Nonnull final Map<String, FhirView> resolvedViews,
      @Nonnull final DataSource dataSource,
      @Nonnull final String requestId) {

    final Map<String, String> labelToViewName = new LinkedHashMap<>();

    for (final Map.Entry<String, FhirView> entry : resolvedViews.entrySet()) {
      final String label = entry.getKey();
      final FhirView view = entry.getValue();

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

      final String tempViewName = registerDataset(label, result, requestId);
      labelToViewName.put(label, tempViewName);
      log.info(
          "Registered temporary view '{}' for label '{}' (resource type '{}')",
          tempViewName,
          label,
          view.getResource());
    }

    return labelToViewName;
  }

  /**
   * Registers an already-built dataset under a request-scoped temp view name and returns that name.
   * Visible for tests that need to exercise the temp-view namespacing without going through
   * FhirViewExecutor.
   */
  @Nonnull
  String registerDataset(
      @Nonnull final String label,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String requestId) {
    final String tempViewName = resolveTempViewName(requestId, label);
    dataset.createOrReplaceTempView(tempViewName);
    return tempViewName;
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
   * is replaced with the corresponding temporary view name, matched on word boundaries so that
   * labels appearing as substrings of other identifiers are not affected.
   *
   * @param sql the original SQL query
   * @param labelToViewName the mapping from labels to temporary view names
   * @return the rewritten SQL query
   */
  @Nonnull
  public String rewriteSql(
      @Nonnull final String sql, @Nonnull final Map<String, String> labelToViewName) {

    String rewritten = sql;
    // Replace longest labels first to avoid partial replacements where one label is a prefix of
    // another.
    final var sortedEntries =
        labelToViewName.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()))
            .toList();

    for (final Map.Entry<String, String> entry : sortedEntries) {
      final String label = entry.getKey();
      final String viewName = entry.getValue();
      final Pattern pattern = Pattern.compile("\\b" + Pattern.quote(label) + "\\b");
      final Matcher matcher = pattern.matcher(rewritten);
      rewritten = matcher.replaceAll(Matcher.quoteReplacement(viewName));
    }

    return rewritten;
  }

  /**
   * Constructs the request-scoped temp view name for a given label.
   *
   * <p>The request id is sanitised to keep the resulting identifier valid for use as a Spark temp
   * view name (HAPI default is 16 alphanumerics, but {@code X-Request-ID} can carry arbitrary
   * characters).
   */
  @Nonnull
  static String resolveTempViewName(@Nonnull final String requestId, @Nonnull final String label) {
    return VIEW_NAME_PREFIX + sanitiseRequestId(requestId) + "_" + label;
  }

  /**
   * Strips characters that aren't valid in a Spark identifier. Falls back to a hash of the original
   * input when the result would otherwise be empty (e.g. a request id consisting only of dashes,
   * which would collide with another all-special-chars id and reintroduce the temp-view clobbering
   * this namespacing exists to prevent).
   */
  @Nonnull
  private static String sanitiseRequestId(@Nonnull final String requestId) {
    final String sanitised = UNSAFE_REQUEST_ID_CHARS.matcher(requestId).replaceAll("");
    if (!sanitised.isEmpty()) {
      return sanitised;
    }
    return "r" + Integer.toUnsignedString(requestId.hashCode(), 16);
  }
}
