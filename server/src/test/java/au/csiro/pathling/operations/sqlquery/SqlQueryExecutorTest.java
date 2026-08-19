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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryExecutor}: the row-cap clamp logic in {@link
 * SqlQueryExecutor#effectiveLimit(Integer, String)}, and the translation of Spark analysis failures
 * into a {@code 422 Unprocessable Entity} response.
 *
 * @author John Grimes
 */
class SqlQueryExecutorTest {

  private static final String REQUEST_ID = "test-request";

  private static final String SQL = "SELECT no_such_col FROM t";

  @Test
  void appliesServerCapWhenCallerHasNoLimit() {
    final SqlQueryExecutor executor = newExecutor(2);
    assertThat(executor.effectiveLimit(null, REQUEST_ID)).isEqualTo(2);
  }

  @Test
  void appliesCallerLimitWhenLowerThanCap() {
    final SqlQueryExecutor executor = newExecutor(1000);
    assertThat(executor.effectiveLimit(5, REQUEST_ID)).isEqualTo(5);
  }

  @Test
  void appliesServerCapWhenCallerLimitExceedsIt() {
    final SqlQueryExecutor executor = newExecutor(10);
    assertThat(executor.effectiveLimit(1_000_000, REQUEST_ID)).isEqualTo(10);
  }

  @Test
  void clampsConfiguredCapToIntegerMaxValue() {
    // A "disable the cap" value larger than Integer.MAX_VALUE must clamp down so it can be passed
    // to Spark's Dataset.limit(int) API.
    final SqlQueryExecutor executor = newExecutor(Long.MAX_VALUE);
    assertThat(executor.effectiveLimit(null, REQUEST_ID)).isEqualTo(Integer.MAX_VALUE);
  }

  // -------------------------------------------------------------------------
  // Spark analysis failures translated to 422 (US1, FR-001/FR-003/FR-008)
  // -------------------------------------------------------------------------

  @Test
  void analysisFailureCarriesSingleInvalidIssueWithPrefixedDiagnostics() {
    // The 422 outcome must carry exactly one issue whose severity is error and whose code is
    // invalid, with the prefixed analyser message as diagnostics. HAPI's default outcome for this
    // exception uses code = processing, which loses the distinction between a client mistake and a
    // processing fault, hence the hand-built outcome.
    final String diagnostics =
        "SQL analysis failed: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function"
            + " parameter with name `no_such_col` cannot be resolved.";

    final UnprocessableEntityException exception = SqlQueryExecutor.analysisFailure(diagnostics);

    assertThat(exception.getStatusCode()).isEqualTo(422);
    assertThat(exception.getMessage()).isEqualTo(diagnostics);
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome).isNotNull();
    assertThat(outcome.getIssue()).hasSize(1);
    assertThat(outcome.getIssueFirstRep().getSeverity()).isEqualTo(IssueSeverity.ERROR);
    assertThat(outcome.getIssueFirstRep().getCode()).isEqualTo(IssueType.INVALID);
    assertThat(outcome.getIssueFirstRep().getDiagnostics()).isEqualTo(diagnostics);
    // The faulting element is the SQL text rather than a request parameter, so no expression is
    // set.
    assertThat(outcome.getIssueFirstRep().getExpression()).isEmpty();
  }

  @Test
  void analysisFailureDuringTopLevelExecutionSurfacesAs422() {
    // An AnalysisException raised while analysing the top-level SQL is translated at the point of
    // execution, so the client receives a 422 rather than the opaque 500 that the error-handling
    // interceptor would otherwise produce.
    final AnalysisException analysisException = mock(AnalysisException.class);
    when(analysisException.getMessage()).thenReturn("[UNRESOLVED_COLUMN] boom");

    assertThatThrownBy(() -> executeWithFailingSql(analysisException, /* timedOut= */ false))
        .isInstanceOf(UnprocessableEntityException.class)
        .hasMessage("SQL analysis failed: [UNRESOLVED_COLUMN] boom");
  }

  @Test
  void watchdogTimeoutTakesPrecedenceOverAnalysisFailure() {
    // Cancelling a query mid-analysis can surface as an AnalysisException. The watchdog handler
    // must still win, so a timed-out query reports the timeout rather than being misclassified as
    // a client SQL mistake (FR-008).
    final AnalysisException analysisException = mock(AnalysisException.class);
    when(analysisException.getMessage()).thenReturn("[UNRESOLVED_COLUMN] boom");

    assertThatThrownBy(() -> executeWithFailingSql(analysisException, /* timedOut= */ true))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Query exceeded the configured timeout of 7 seconds.");
  }

  /**
   * Runs {@link SqlQueryExecutor#execute} over an empty dependency graph with a Spark session whose
   * {@code sql} call throws the given exception, and a watchdog whose watch reports the given
   * timeout state.
   */
  private static void executeWithFailingSql(
      @Nonnull final Exception sqlFailure, final boolean timedOut) {
    final SparkSession sparkSession = mock(SparkSession.class);
    when(sparkSession.sparkContext()).thenReturn(mock(SparkContext.class));
    // Mockito's thenThrow rejects a checked exception that the stubbed method does not declare, and
    // Scala-compiled methods declare none, so the throw is staged through an Answer instead.
    doAnswer(
            invocation -> {
              throw sqlFailure;
            })
        .when(sparkSession)
        .sql(anyString());

    final ViewRegistrationService viewRegistrationService = mock(ViewRegistrationService.class);
    when(viewRegistrationService.rewriteSql(anyString(), any())).thenReturn(SQL);

    final SqlQueryWatchdog watchdog = mock(SqlQueryWatchdog.class);
    when(watchdog.start(anyString()))
        .thenReturn(
            new SqlQueryWatchdog.Watch(mock(ScheduledFuture.class), new AtomicBoolean(timedOut)));

    final SqlQueryConfiguration sqlQueryConfig = new SqlQueryConfiguration();
    sqlQueryConfig.setTimeoutSeconds(7);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    serverConfiguration.setSqlQuery(sqlQueryConfig);
    final SqlQueryExecutor executor =
        new SqlQueryExecutor(
            sparkSession,
            viewRegistrationService,
            mock(SqlValidator.class),
            serverConfiguration,
            watchdog);

    final SqlQueryRequest request =
        new SqlQueryRequest(
            new ParsedSqlQuery(SQL, List.of(), List.of(), SqlLibraryParser.SQL_QUERY_TYPE_CODE),
            SqlQueryOutputFormat.NDJSON,
            /* includeHeader= */ false,
            null,
            Map.of());
    executor.execute(
        request,
        new ResolvedDependencyGraph(List.of(), Map.of(), Map.of()),
        mock(DataSource.class),
        REQUEST_ID,
        dataset -> {
          throw new AssertionError("The terminal consumer must not be reached.");
        });
  }

  @Nonnull
  private static SqlQueryExecutor newExecutor(final long maxRows) {
    final SqlQueryConfiguration sqlQueryConfig = new SqlQueryConfiguration();
    sqlQueryConfig.setMaxRows(maxRows);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    serverConfiguration.setSqlQuery(sqlQueryConfig);
    return new SqlQueryExecutor(
        mock(SparkSession.class),
        mock(ViewRegistrationService.class),
        mock(SqlValidator.class),
        serverConfiguration,
        mock(SqlQueryWatchdog.class));
  }
}
