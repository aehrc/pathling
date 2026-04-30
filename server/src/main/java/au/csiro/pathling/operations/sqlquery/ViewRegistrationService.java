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
   * Rewrites SQL table references to use the prefixed temporary view names. Each unquoted
   * identifier token whose text equals a label is replaced with the corresponding temporary view
   * name; backtick-quoted identifiers matching a label are also rewritten. Single-quoted and
   * double-quoted string literals, line comments ({@code -- ...}), and block comments ({@code /*
   * ... *}{@code /}) are left untouched, so a literal value happening to equal a label (e.g. {@code
   * WHERE x = 'patients'}) is preserved.
   *
   * @param sql the original SQL query
   * @param labelToViewName the mapping from labels to temporary view names
   * @return the rewritten SQL query
   */
  @Nonnull
  public String rewriteSql(
      @Nonnull final String sql, @Nonnull final Map<String, String> labelToViewName) {

    if (labelToViewName.isEmpty()) {
      return sql;
    }

    final int n = sql.length();
    final StringBuilder out = new StringBuilder(n);
    int i = 0;
    while (i < n) {
      final char c = sql.charAt(i);

      if (c == '\'' || c == '"') {
        i = copyStringLiteral(sql, i, c, out);
        continue;
      }
      if (c == '`') {
        i = copyOrRewriteBacktickIdentifier(sql, i, labelToViewName, out);
        continue;
      }
      if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        i = copyLineComment(sql, i, out);
        continue;
      }
      if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        i = copyBlockComment(sql, i, out);
        continue;
      }
      if (Character.isLetter(c) || c == '_') {
        i = copyOrRewriteIdentifier(sql, i, labelToViewName, out);
        continue;
      }

      out.append(c);
      i++;
    }
    return out.toString();
  }

  /**
   * Copies a {@code '...'} or {@code "..."} string literal verbatim, honouring both doubled-quote
   * and backslash escapes (Spark accepts either). Returns the position after the closing quote.
   */
  private static int copyStringLiteral(
      @Nonnull final String sql,
      final int start,
      final char quote,
      @Nonnull final StringBuilder out) {
    final int n = sql.length();
    out.append(quote);
    int i = start + 1;
    while (i < n) {
      final char d = sql.charAt(i);
      if (d == '\\' && i + 1 < n) {
        out.append(d).append(sql.charAt(i + 1));
        i += 2;
        continue;
      }
      if (d == quote) {
        if (i + 1 < n && sql.charAt(i + 1) == quote) {
          out.append(d).append(d);
          i += 2;
          continue;
        }
        out.append(d);
        return i + 1;
      }
      out.append(d);
      i++;
    }
    return i;
  }

  /**
   * A backtick-quoted identifier is a delimited identifier in Spark SQL. If its text matches a
   * label, rewrite it to the (unquoted) temp view name; otherwise copy it verbatim.
   */
  private static int copyOrRewriteBacktickIdentifier(
      @Nonnull final String sql,
      final int start,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final StringBuilder out) {
    final int n = sql.length();
    int j = start + 1;
    while (j < n && sql.charAt(j) != '`') {
      j++;
    }
    if (j >= n) {
      // Unterminated backtick — copy the rest verbatim and stop.
      out.append(sql, start, n);
      return n;
    }
    final String inner = sql.substring(start + 1, j);
    final String replacement = labelToViewName.get(inner);
    if (replacement != null) {
      out.append(replacement);
    } else {
      out.append(sql, start, j + 1);
    }
    return j + 1;
  }

  /** Copies a {@code -- ...} line comment up to (and including) the terminating newline. */
  private static int copyLineComment(
      @Nonnull final String sql, final int start, @Nonnull final StringBuilder out) {
    final int n = sql.length();
    int i = start;
    while (i < n && sql.charAt(i) != '\n') {
      out.append(sql.charAt(i));
      i++;
    }
    return i;
  }

  /** Copies a {@code /* ... *}{@code /} block comment verbatim. */
  private static int copyBlockComment(
      @Nonnull final String sql, final int start, @Nonnull final StringBuilder out) {
    final int n = sql.length();
    out.append("/*");
    int i = start + 2;
    while (i + 1 < n) {
      if (sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
        out.append("*/");
        return i + 2;
      }
      out.append(sql.charAt(i));
      i++;
    }
    while (i < n) {
      out.append(sql.charAt(i));
      i++;
    }
    return i;
  }

  /**
   * Reads an unquoted identifier starting at {@code start} and replaces it with the matching temp
   * view name if the token equals a known label.
   */
  private static int copyOrRewriteIdentifier(
      @Nonnull final String sql,
      final int start,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final StringBuilder out) {
    final int n = sql.length();
    int j = start;
    while (j < n) {
      final char ch = sql.charAt(j);
      if (Character.isLetterOrDigit(ch) || ch == '_') {
        j++;
      } else {
        break;
      }
    }
    final String token = sql.substring(start, j);
    final String replacement = labelToViewName.get(token);
    out.append(replacement != null ? replacement : token);
    return j;
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
