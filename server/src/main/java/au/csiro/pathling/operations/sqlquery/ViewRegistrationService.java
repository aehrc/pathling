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
 * views to the HAPI per-request id and keys them by the resolved dependency's canonical identity,
 * so concurrent {@code $sqlquery-run} requests cannot clobber one another in Spark's session-global
 * temporary view catalog, a shared node materialises once, and the same table label used in
 * different nodes cannot collide.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ViewRegistrationService {

  private static final String VIEW_NAME_PREFIX = "sqlquery_";

  private static final Pattern UNSAFE_REQUEST_ID_CHARS = Pattern.compile("\\W");

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
   * Registers an already-built dataset under a request-scoped temp view name derived from the given
   * identifier (a dependency's canonical key, or a bare label in the single-level tests) and
   * returns that name.
   *
   * @param identifier the canonical key (or label) the temp view materialises
   * @param dataset the dataset to register
   * @param requestId the per-request id used to namespace the registered view name
   * @return the registered temp view name
   */
  @Nonnull
  String registerDataset(
      @Nonnull final String identifier,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String requestId) {
    final String tempViewName = resolveTempViewName(requestId, identifier);
    dataset.createOrReplaceTempView(tempViewName);
    return tempViewName;
  }

  /**
   * Builds the dataset for a resolved {@code ViewDefinition} leaf by executing its parsed view
   * against the data source. The result is not registered; the caller registers it once any
   * required validation has passed.
   *
   * @param view the parsed view to execute
   * @param dataSource the data source backing view execution
   * @return the view's result dataset
   * @throws InvalidRequestException if the view cannot be executed
   */
  @Nonnull
  public Dataset<Row> buildViewDefinition(
      @Nonnull final FhirView view, @Nonnull final DataSource dataSource) {
    final FhirViewExecutor executor =
        new FhirViewExecutor(fhirContext, dataSource, queryConfiguration);
    try {
      return executor.buildQuery(view);
    } catch (final Exception e) {
      throw new InvalidRequestException(
          "Failed to execute ViewDefinition for resource type '"
              + view.getResource()
              + "': "
              + e.getMessage());
    }
  }

  /**
   * Builds the dataset for a resolved {@code SQLView} node by rewriting its SQL so each of its
   * table labels points at the already-registered temp view of the child it resolves to, then
   * running that SQL. The result is not registered; the caller validates the analysed plan and then
   * registers it.
   *
   * @param node the resolved SQLView node
   * @param registeredByKey the temp view names of already-materialised nodes, keyed by canonical
   *     key
   * @return the SQLView's result dataset
   */
  @Nonnull
  public Dataset<Row> buildSqlView(
      @Nonnull final ResolvedSqlView node, @Nonnull final Map<String, String> registeredByKey) {
    final Map<String, String> labelToViewName = new LinkedHashMap<>();
    node.getChildKeysByLabel()
        .forEach(
            (label, childKey) -> {
              final String viewName = registeredByKey.get(childKey);
              if (viewName == null) {
                // Topological ordering guarantees children are materialised first; a miss here is
                // an internal invariant violation, not a client error.
                throw new IllegalStateException(
                    "Child dependency '"
                        + childKey
                        + "' for label '"
                        + label
                        + "' was not materialised before the SQLView that references it");
              }
              labelToViewName.put(label, viewName);
            });
    final String rewrittenSql = rewriteSql(node.getSql(), labelToViewName);
    return sparkSession.sql(rewrittenSql);
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
      i = consumeNextToken(sql, n, i, labelToViewName, out);
    }
    return out.toString();
  }

  /** Consumes one token starting at {@code i} and returns the index after it. */
  private static int consumeNextToken(
      @Nonnull final String sql,
      final int n,
      final int i,
      @Nonnull final Map<String, String> labelToViewName,
      @Nonnull final StringBuilder out) {
    final char c = sql.charAt(i);
    if (c == '\'' || c == '"') {
      return copyStringLiteral(sql, i, c, out);
    }
    if (c == '`') {
      return copyOrRewriteBacktickIdentifier(sql, i, labelToViewName, out);
    }
    if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
      return copyLineComment(sql, i, out);
    }
    if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
      return copyBlockComment(sql, i, out);
    }
    if (Character.isLetter(c) || c == '_') {
      return copyOrRewriteIdentifier(sql, i, labelToViewName, out);
    }
    out.append(c);
    return i + 1;
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
      } else if (d == quote && i + 1 < n && sql.charAt(i + 1) == quote) {
        // Doubled quote inside the literal — Spark treats this as an escaped quote.
        out.append(d).append(d);
        i += 2;
      } else if (d == quote) {
        out.append(d);
        return i + 1;
      } else {
        out.append(d);
        i++;
      }
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
   * Constructs the request-scoped temp view name for a node identifier. The identifier is either a
   * dependency's canonical key (the production case, so a shared node materialises once and labels
   * cannot collide across nodes) or, for the existing single-level tests, a bare table label.
   *
   * <p>Both the request id and the identifier are sanitised so that the resulting Spark temp view
   * name is a legal identifier (HAPI request ids are alphanumeric, but {@code X-Request-ID} and
   * canonical keys such as {@code ViewDefinition/patient-view} can carry slashes and dashes).
   *
   * @param requestId the per-request id used to namespace registered view names
   * @param identifier the canonical key (or label) the temp view materialises
   * @return the temp view name
   */
  @Nonnull
  static String resolveTempViewName(
      @Nonnull final String requestId, @Nonnull final String identifier) {
    return VIEW_NAME_PREFIX + sanitiseRequestId(requestId) + "_" + sanitiseIdentifier(identifier);
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

  /**
   * Renders a node identifier as a Spark-safe temp view name segment. A bare word identifier (a
   * validated table label) is used verbatim, preserving the single-level naming. Any identifier
   * containing characters illegal in a Spark identifier (a canonical key such as {@code
   * ViewDefinition/patient-view}) has those characters replaced with underscores and a hash of the
   * original appended, so that two distinct keys never collapse to the same name.
   */
  @Nonnull
  private static String sanitiseIdentifier(@Nonnull final String identifier) {
    if (!UNSAFE_REQUEST_ID_CHARS.matcher(identifier).find()) {
      return identifier;
    }
    final String cleaned = UNSAFE_REQUEST_ID_CHARS.matcher(identifier).replaceAll("_");
    return cleaned + "_" + Integer.toUnsignedString(identifier.hashCode(), 16);
  }
}
