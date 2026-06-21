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
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.operations.export.ExportManifestOutput;
import au.csiro.pathling.operations.view.ViewExportFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryExportExecutor}, verifying that it produces one named output per
 * query and writes via the shared file writer. The Spark-backed collaborators are mocked.
 *
 * @author John Grimes
 */
class SqlQueryExportExecutorTest {

  private SqlQueryPipeline pipeline;
  private QueryableDataSource deltaLake;
  private ExportDataSourceBuilder dataSourceBuilder;
  private ExportFileWriter fileWriter;
  private SqlQueryExportExecutor executor;
  private Dataset<Row> dataset;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    pipeline = mock(SqlQueryPipeline.class);
    deltaLake = mock(QueryableDataSource.class);
    dataSourceBuilder = mock(ExportDataSourceBuilder.class);
    fileWriter = mock(ExportFileWriter.class);
    executor = new SqlQueryExportExecutor(pipeline, deltaLake, dataSourceBuilder, fileWriter);
    dataset = mock(Dataset.class);

    when(dataSourceBuilder.build(any(), any(), anySet())).thenReturn(deltaLake);
    when(fileWriter.createJobDirectory(any())).thenReturn(new Path("file:///tmp/jobs/job-1"));
    // uniqueName returns the requested base name unchanged for these single-output cases.
    when(fileWriter.uniqueName(any(), anySet()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    // The pipeline invokes the terminal consumer with the dataset, as the real one does.
    doAnswer(
            invocation -> {
              final Consumer<Dataset<Row>> consumer = invocation.getArgument(3);
              consumer.accept(dataset);
              return null;
            })
        .when(pipeline)
        .execute(any(), any(), any(), any());
  }

  @Test
  void singleQueryYieldsOneNamedOutputWithFileUrls() {
    final List<String> fileUrls = List.of("file:///tmp/jobs/job-1/patients.ndjson/part-00000.json");
    when(fileWriter.writeNdjson(eq(dataset), eq("patients"), any())).thenReturn(fileUrls);

    final SqlQueryExportRequest request = request(ViewExportFormat.NDJSON, queryInput("patients"));

    final List<ExportManifestOutput> outputs = executor.execute(request, "job-1");

    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0).name()).isEqualTo("patients");
    assertThat(outputs.get(0).fileUrls()).isEqualTo(fileUrls);
  }

  @Test
  void csvFormatRoutesToCsvWriterWithHeaderFlag() {
    final List<String> fileUrls = List.of("file:///tmp/jobs/job-1/patients.csv/part-00000.csv");
    when(fileWriter.writeCsv(eq(dataset), eq("patients"), eq(false), any())).thenReturn(fileUrls);

    final SqlQueryExportRequest request =
        new SqlQueryExportRequest(
            "http://x/$sqlquery-export",
            "http://x/fhir",
            List.of(queryInput("patients")),
            null,
            ViewExportFormat.CSV,
            /* includeHeader= */ false,
            java.util.Set.of(),
            null);

    final List<ExportManifestOutput> outputs = executor.execute(request, "job-1");

    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0).fileUrls()).isEqualTo(fileUrls);
  }

  @Test
  void partitionedResultYieldsOneOutputWithAllFileUrls() {
    // A query partitioned into multiple files produces a single output that carries every file URL;
    // the manifest builder then repeats the location part once per file (FR-028).
    final List<String> fileUrls =
        List.of(
            "file:///tmp/jobs/job-1/people.00000.ndjson",
            "file:///tmp/jobs/job-1/people.00001.ndjson");
    when(fileWriter.writeNdjson(eq(dataset), eq("people"), any())).thenReturn(fileUrls);

    final List<ExportManifestOutput> outputs =
        executor.execute(request(ViewExportFormat.NDJSON, queryInput("people")), "job-1");

    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0).fileUrls()).containsExactlyElementsOf(fileUrls);
  }

  @Test
  void outputNameFollowsPrecedenceQueryNameThenLibraryNameThenGenerated() {
    final PreparedSqlQuery prepared =
        new PreparedSqlQuery(
            new SqlQueryRequest(
                new ParsedSqlQuery("SELECT 1", List.of(), List.of()),
                SqlQueryOutputFormat.NDJSON,
                true,
                null,
                Map.of()),
            Map.of());

    // query.name wins.
    assertThat(new QueryInput("explicit", "lib", prepared).getEffectiveName(0))
        .isEqualTo("explicit");
    // Library name is the next fallback.
    assertThat(new QueryInput(null, "lib_name", prepared).getEffectiveName(0))
        .isEqualTo("lib_name");
    // A generated name is the final fallback.
    assertThat(new QueryInput(null, null, prepared).getEffectiveName(2)).isEqualTo("query_2");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static SqlQueryExportRequest request(
      final ViewExportFormat format, final QueryInput... queries) {
    return new SqlQueryExportRequest(
        "http://x/$sqlquery-export",
        "http://x/fhir",
        List.of(queries),
        null,
        format,
        true,
        java.util.Set.of(),
        null);
  }

  private static QueryInput queryInput(final String name) {
    final ParsedSqlQuery parsedQuery =
        new ParsedSqlQuery("SELECT id FROM patients", List.of(), List.of());
    final SqlQueryRequest request =
        new SqlQueryRequest(parsedQuery, SqlQueryOutputFormat.NDJSON, true, null, Map.of());
    return new QueryInput(name, null, new PreparedSqlQuery(request, Map.of()));
  }
}
