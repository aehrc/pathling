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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.views.FhirView;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryPipeline}, verifying that it orchestrates parsing, view resolution,
 * static validation, and execution across its collaborators. The collaborators are mocked, so the
 * test exercises the pipeline's wiring rather than the Spark-backed execution itself.
 *
 * @author John Grimes
 */
class SqlQueryPipelineTest {

  private SqlQueryRequestParser requestParser;
  private ViewResolver viewResolver;
  private SqlQueryExecutor executor;
  private SqlValidator sqlValidator;
  private SqlQueryPipeline pipeline;

  private Library library;
  private SqlQueryRequest request;
  private Map<String, FhirView> resolvedViews;

  @BeforeEach
  void setUp() {
    requestParser = mock(SqlQueryRequestParser.class);
    viewResolver = mock(ViewResolver.class);
    executor = mock(SqlQueryExecutor.class);
    sqlValidator = mock(SqlValidator.class);
    pipeline = new SqlQueryPipeline(requestParser, viewResolver, executor, sqlValidator);

    library = new Library();
    final ParsedSqlQuery parsedQuery =
        new ParsedSqlQuery(
            "SELECT id FROM patients",
            List.of(new ViewArtifactReference("patients", "ViewDefinition/patient-view")),
            List.of());
    request = new SqlQueryRequest(parsedQuery, SqlQueryOutputFormat.NDJSON, true, null, Map.of());
    final FhirView view = mock(FhirView.class);
    resolvedViews = Map.of("patients", view);
  }

  @Test
  void prepareParsesAndResolvesViewsWithSuppliedViews() {
    final FhirView suppliedView = mock(FhirView.class);
    final Map<String, FhirView> supplied = Map.of("patient-view", suppliedView);
    when(requestParser.parse(eq(library), eq("ndjson"), any(), any(), any(), any()))
        .thenReturn(request);
    when(viewResolver.resolve(request.getParsedQuery().getViewReferences(), supplied))
        .thenReturn(resolvedViews);

    final PreparedSqlQuery prepared =
        pipeline.prepare(library, "ndjson", null, null, null, null, supplied);

    assertThat(prepared.getRequest()).isSameAs(request);
    assertThat(prepared.getResolvedViews()).isEqualTo(resolvedViews);
    verify(requestParser).parse(eq(library), eq("ndjson"), any(), any(), any(), any());
    verify(viewResolver).resolve(request.getParsedQuery().getViewReferences(), supplied);
  }

  @Test
  void validateStaticallyValidatesSqlWithDeclaredLabels() {
    final PreparedSqlQuery prepared = new PreparedSqlQuery(request, resolvedViews);

    pipeline.validateStatically(prepared);

    verify(sqlValidator).validate("SELECT id FROM patients", Set.of("patients"));
  }

  @Test
  void executeDelegatesToExecutorAndPassesDatasetToConsumer() {
    final PreparedSqlQuery prepared = new PreparedSqlQuery(request, resolvedViews);
    final DataSource dataSource = mock(DataSource.class);
    @SuppressWarnings("unchecked")
    final Dataset<Row> dataset = mock(Dataset.class);

    // Have the mocked executor invoke the terminal consumer with the dataset, as the real one does.
    doAnswer(
            invocation -> {
              final Consumer<Dataset<Row>> consumer = invocation.getArgument(4);
              consumer.accept(dataset);
              return null;
            })
        .when(executor)
        .execute(eq(request), eq(resolvedViews), eq(dataSource), eq("req-1"), any());

    final AtomicReference<Dataset<Row>> received = new AtomicReference<>();
    pipeline.execute(prepared, dataSource, "req-1", received::set);

    assertThat(received.get()).isSameAs(dataset);
    verify(executor).execute(eq(request), eq(resolvedViews), eq(dataSource), eq("req-1"), any());
  }
}
