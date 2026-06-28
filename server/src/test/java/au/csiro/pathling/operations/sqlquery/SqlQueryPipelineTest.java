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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Library;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryPipeline}, verifying that it orchestrates parsing, dependency
 * resolution, static validation, and execution across its collaborators. The collaborators are
 * mocked, so the test exercises the pipeline's wiring rather than the Spark-backed execution
 * itself.
 *
 * @author John Grimes
 */
class SqlQueryPipelineTest {

  private SqlQueryRequestParser requestParser;
  private SqlDependencyResolver dependencyResolver;
  private SqlQueryExecutor executor;
  private SqlQueryPipeline pipeline;

  private Library library;
  private SqlQueryRequest request;
  private ResolvedDependencyGraph graph;

  @BeforeEach
  void setUp() {
    requestParser = mock(SqlQueryRequestParser.class);
    dependencyResolver = mock(SqlDependencyResolver.class);
    executor = mock(SqlQueryExecutor.class);
    pipeline = new SqlQueryPipeline(requestParser, dependencyResolver, executor);

    library = new Library();
    final ParsedSqlQuery parsedQuery =
        new ParsedSqlQuery(
            "SELECT id FROM patients",
            List.of(new ViewArtifactReference("patients", "ViewDefinition/patient-view")),
            List.of(),
            SqlLibraryParser.SQL_QUERY_TYPE_CODE);
    request = new SqlQueryRequest(parsedQuery, SqlQueryOutputFormat.NDJSON, true, null, Map.of());
    final ResolvedViewDefinition leaf =
        new ResolvedViewDefinition("ViewDefinition/patient-view", mock(FhirView.class));
    graph =
        new ResolvedDependencyGraph(
            List.of(leaf),
            Map.of("patients", "ViewDefinition/patient-view"),
            Map.of("ViewDefinition/patient-view", leaf));
  }

  @Test
  void prepareParsesAndResolvesDependencyGraphWithSuppliedViews() {
    final FhirView suppliedView = mock(FhirView.class);
    final Map<String, FhirView> supplied = Map.of("patient-view", suppliedView);
    when(requestParser.parse(eq(library), eq("ndjson"), any(), any(), any(), any()))
        .thenReturn(request);
    when(dependencyResolver.resolve(request.getParsedQuery(), supplied)).thenReturn(graph);

    final PreparedSqlQuery prepared =
        pipeline.prepare(library, "ndjson", null, null, null, null, supplied);

    assertThat(prepared.getRequest()).isSameAs(request);
    assertThat(prepared.getDependencyGraph()).isSameAs(graph);
    verify(requestParser).parse(eq(library), eq("ndjson"), any(), any(), any(), any());
    verify(dependencyResolver).resolve(request.getParsedQuery(), supplied);
  }

  @Test
  void validateStaticallyDelegatesToExecutor() {
    final PreparedSqlQuery prepared = new PreparedSqlQuery(request, graph);

    pipeline.validateStatically(prepared);

    verify(executor).validateStatically(request, graph);
  }

  @Test
  void executeDelegatesToExecutorAndPassesDatasetToConsumer() {
    final PreparedSqlQuery prepared = new PreparedSqlQuery(request, graph);
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
        .execute(eq(request), eq(graph), eq(dataSource), eq("req-1"), any());

    final AtomicReference<Dataset<Row>> received = new AtomicReference<>();
    pipeline.execute(prepared, dataSource, "req-1", received::set);

    assertThat(received.get()).isSameAs(dataset);
    verify(executor).execute(eq(request), eq(graph), eq(dataSource), eq("req-1"), any());
  }
}
