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

package au.csiro.pathling.operations.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.io.SnapshotDeltaSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.operations.export.ExportManifestOutput;
import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.operations.sqlquery.SqlQueryPipeline;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link SqlExportExecutor}, covering the job-level guarantees of
 * contracts/sql-export.md: one output per subject, every subject reading through one snapshot
 * captured at execution start, the format and header settings reaching the file writer, and the
 * all-or-nothing failure behaviour.
 *
 * <p>The SQL engine and the file writer are mocked, so these tests exercise the executor's own
 * orchestration. The ViewDefinition path builds its query plan against a live Spark session and is
 * covered by {@link SqlExportProviderIT} instead.
 *
 * @author John Grimes
 */
class SqlExportExecutorTest {

  private static final String JOB_ID = "job-1";

  private SqlQueryPipeline pipeline;
  private DynamicDeltaSource deltaLake;
  private SnapshotDeltaSource snapshot;
  private ExportDataSourceBuilder dataSourceBuilder;
  private ExportFileWriter fileWriter;
  private QueryableDataSource filteredSource;
  private SqlExportExecutor executor;

  @BeforeEach
  void setUp() {
    pipeline = mock(SqlQueryPipeline.class);
    deltaLake = mock(DynamicDeltaSource.class);
    snapshot = mock(SnapshotDeltaSource.class);
    dataSourceBuilder = mock(ExportDataSourceBuilder.class);
    fileWriter = mock(ExportFileWriter.class);
    filteredSource = mock(QueryableDataSource.class);

    when(deltaLake.snapshot()).thenReturn(snapshot);
    when(dataSourceBuilder.build(any(), any(), any())).thenReturn(filteredSource);
    when(fileWriter.createJobDirectory(any())).thenReturn(new Path("/tmp/" + JOB_ID));
    when(fileWriter.writeNdjson(any(), any(), any())).thenReturn(List.of("file.ndjson"));
    when(fileWriter.writeCsv(any(), any(), anyBoolean(), any())).thenReturn(List.of("file.csv"));

    // The pipeline hands the result to the caller's consumer, which is where the write happens.
    doAnswer(
            invocation -> {
              invocation
                  .getArgument(3, Consumer.class)
                  .accept(mock(Dataset.class, org.mockito.Answers.RETURNS_DEEP_STUBS));
              return null;
            })
        .when(pipeline)
        .execute(any(), any(), any(), any());

    executor =
        new SqlExportExecutor(
            pipeline,
            deltaLake,
            mock(FhirContext.class),
            mock(ServerConfiguration.class, org.mockito.Answers.RETURNS_DEEP_STUBS),
            dataSourceBuilder,
            fileWriter);
  }

  // Each subject produces exactly one manifest output, named as the parser resolved it. Context
  // entries never reach the executor, so they cannot produce an output of their own.
  @Test
  void producesExactlyOneOutputPerSubject() {
    final List<ExportManifestOutput> outputs =
        executor.execute(request(sqlSubject("first"), sqlSubject("second")), JOB_ID);

    assertThat(outputs).hasSize(2);
    assertThat(outputs).extracting(ExportManifestOutput::name).containsExactly("first", "second");
    assertThat(outputs).allSatisfy(o -> assertThat(o.fileUrls()).isNotEmpty());
  }

  // Every subject is run from its already-prepared query, so the bindings the parser bound at
  // kick-off are the ones that execute.
  @Test
  void runsEachSubjectFromItsPreparedQuery() {
    final SubjectInput first = sqlSubject("first");
    final SubjectInput second = sqlSubject("second");

    executor.execute(request(first, second), JOB_ID);

    verify(pipeline).execute(eq(first.preparedQuery()), any(), any(), any());
    verify(pipeline).execute(eq(second.preparedQuery()), any(), any(), any());
  }

  // The whole job reads through one snapshot, captured once at execution start, so a write that
  // lands between two subjects' executions is invisible to the second.
  @Test
  void readsEverySubjectThroughOneSnapshotCapturedAtStart() {
    executor.execute(request(sqlSubject("first"), sqlSubject("second")), JOB_ID);

    verify(deltaLake, times(1)).snapshot();

    final ArgumentCaptor<QueryableDataSource> base =
        ArgumentCaptor.forClass(QueryableDataSource.class);
    verify(dataSourceBuilder, times(1)).build(base.capture(), any(), any());
    assertThat(base.getValue()).isSameAs(snapshot);

    // The filtered snapshot, not the live source, is what each subject executes against.
    verify(pipeline, times(2)).execute(any(), eq(filteredSource), any(), any());
  }

  // A source with no Delta history to travel through - a substituted in-memory source - is read
  // live rather than failing the job.
  @Test
  void readsAnUnpinnableSourceLive() {
    final QueryableDataSource plainSource = mock(QueryableDataSource.class);
    final SqlExportExecutor plainExecutor =
        new SqlExportExecutor(
            pipeline,
            plainSource,
            mock(FhirContext.class),
            mock(ServerConfiguration.class, org.mockito.Answers.RETURNS_DEEP_STUBS),
            dataSourceBuilder,
            fileWriter);

    plainExecutor.execute(request(sqlSubject("only")), JOB_ID);

    final ArgumentCaptor<QueryableDataSource> base =
        ArgumentCaptor.forClass(QueryableDataSource.class);
    verify(dataSourceBuilder).build(base.capture(), any(), any());
    assertThat(base.getValue()).isSameAs(plainSource);
  }

  // Each subject's temp views are namespaced within the job, so two subjects of one job cannot
  // collide on a request-scoped view name.
  @Test
  void namespacesEachSubjectsTempViewsWithinTheJob() {
    executor.execute(request(sqlSubject("first"), sqlSubject("second")), JOB_ID);

    verify(pipeline).execute(any(), any(), eq(JOB_ID + "-0"), any());
    verify(pipeline).execute(any(), any(), eq(JOB_ID + "-1"), any());
  }

  // The job's format decides how the result is written, and the header flag reaches the CSV writer.
  @Test
  void writesCsvWithTheRequestedHeaderSetting() {
    executor.execute(
        new SqlExportRequest(
            "http://localhost/fhir/$sql-export",
            "http://localhost/fhir",
            List.of(sqlSubject("only")),
            null,
            SqlExportFormat.CSV,
            false,
            Set.of(),
            null),
        JOB_ID);

    verify(fileWriter).writeCsv(any(), eq("only"), eq(false), any());
    verify(fileWriter, never()).writeNdjson(any(), any(), any());
  }

  // Execution is all-or-nothing: a subject that fails propagates, so the caller can discard the
  // whole job rather than offering a partial export for download.
  @Test
  void propagatesAFailingSubject() {
    doAnswer(
            invocation -> {
              throw new IllegalStateException("Spark said no");
            })
        .when(pipeline)
        .execute(any(), any(), any(), any());

    assertThatThrownBy(() -> executor.execute(request(sqlSubject("only")), JOB_ID))
        .isInstanceOf(IllegalStateException.class);
  }

  // ---- helpers ----

  @Nonnull
  private static SqlExportRequest request(@Nonnull final SubjectInput... subjects) {
    return new SqlExportRequest(
        "http://localhost/fhir/$sql-export",
        "http://localhost/fhir",
        List.of(subjects),
        null,
        SqlExportFormat.NDJSON,
        true,
        Set.of(),
        null);
  }

  @Nonnull
  private static SubjectInput sqlSubject(@Nonnull final String name) {
    return SubjectInput.ofSql(SubjectKind.SQL_QUERY, name, mock(PreparedSqlQuery.class));
  }

  /** Silences the unused-variable warning for the deep-stubbed Row type. */
  @SuppressWarnings("unused")
  private static Dataset<Row> unusedRowType() {
    return null;
  }
}
