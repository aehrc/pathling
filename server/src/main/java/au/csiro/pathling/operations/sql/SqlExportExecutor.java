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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.ParquetSchemaValidator;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.operations.export.ExportManifestOutput;
import au.csiro.pathling.operations.sqlquery.SqlQueryPipeline;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Runs the subjects of an {@code $sql-export} job and writes one output per subject.
 *
 * <p>Every subject is computed against a single snapshot of the data, captured when execution
 * begins: a job that takes minutes to run must not produce outputs that disagree with one another
 * because a write landed halfway through. The snapshot pins each resource type's Delta version, so
 * pinning costs one read of each table's log and nothing is copied.
 *
 * <p>Execution is all-or-nothing. A subject that fails propagates its exception, and the caller
 * discards the whole job, so a partially written export is never offered for download.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class SqlExportExecutor {

  @Nonnull private final SqlQueryPipeline pipeline;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final PathlingContext pathlingContext;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final ExportDataSourceBuilder dataSourceBuilder;

  @Nonnull private final ExportFileWriter fileWriter;

  /**
   * Constructs a new SqlExportExecutor.
   *
   * @param pipeline the SQL evaluation engine
   * @param deltaLake the server's data source, snapshotted at execution start
   * @param pathlingContext the Pathling context, used to hold the snapshot's pinned datasets
   * @param fhirContext the FHIR context, used to build view query plans
   * @param serverConfiguration the server configuration, consulted for the query configuration
   * @param dataSourceBuilder applies the job's filters to the snapshot
   * @param fileWriter creates the job directory and writes the output files
   */
  @SuppressWarnings("java:S107")
  @Autowired
  public SqlExportExecutor(
      @Nonnull final SqlQueryPipeline pipeline,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final ExportDataSourceBuilder dataSourceBuilder,
      @Nonnull final ExportFileWriter fileWriter) {
    this.pipeline = pipeline;
    this.deltaLake = deltaLake;
    this.pathlingContext = pathlingContext;
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
    this.dataSourceBuilder = dataSourceBuilder;
    this.fileWriter = fileWriter;
  }

  /**
   * Executes every subject of the request and writes its output.
   *
   * @param request the validated request
   * @param jobId the id of the job running this export
   * @return the outputs, one per subject, in request order
   */
  @Nonnull
  public List<ExportManifestOutput> execute(
      @Nonnull final SqlExportRequest request, @Nonnull final String jobId) {

    final Path jobDirPath = fileWriter.createJobDirectory(jobId);
    final QueryableDataSource dataSource =
        dataSourceBuilder.build(snapshot(), request.since(), request.patientIds());
    final List<ExportManifestOutput> outputs = new ArrayList<>();

    for (int i = 0; i < request.subjects().size(); i++) {
      final SubjectInput subject = request.subjects().get(i);
      final List<String> fileUrls =
          subject.kind() == SubjectKind.VIEW_DEFINITION
              ? runView(subject, request, dataSource, jobDirPath)
              : runSql(subject, request, dataSource, jobDirPath, jobId + "-" + i);
      outputs.add(new ExportManifestOutput(subject.name(), fileUrls));
    }

    return outputs;
  }

  /**
   * Captures the snapshot every subject of the job reads through. A source that cannot be pinned -
   * a substituted in-memory source in a test, for instance - is used as it is, since it has no
   * Delta history to travel through.
   */
  @Nonnull
  private QueryableDataSource snapshot() {
    if (deltaLake instanceof final DynamicDeltaSource dynamic) {
      return dynamic.snapshot(pathlingContext);
    }
    log.debug("Data source {} cannot be pinned; reading it live", deltaLake.getClass().getName());
    return deltaLake;
  }

  /** Runs a ViewDefinition subject through the FhirView evaluation engine. */
  @Nonnull
  private List<String> runView(
      @Nonnull final SubjectInput subject,
      @Nonnull final SqlExportRequest request,
      @Nonnull final QueryableDataSource dataSource,
      @Nonnull final Path jobDirPath) {
    final FhirView view = Objects.requireNonNull(subject.view());
    final Dataset<Row> result;
    try {
      result =
          new FhirViewExecutor(fhirContext, dataSource, serverConfiguration.getQuery())
              .buildQuery(view);
    } catch (final ConstraintViolationException e) {
      throw SqlOperationError.unprocessable(
          SubjectResolver.SUBJECT_EXPRESSION,
          "The subject '%s' is invalid: %s".formatted(subject.name(), e.getMessage()));
    }
    return writeOutput(result, subject.name(), request, jobDirPath);
  }

  /** Runs a SQLQuery or SQLView subject through the SQL evaluation engine. */
  @Nonnull
  private List<String> runSql(
      @Nonnull final SubjectInput subject,
      @Nonnull final SqlExportRequest request,
      @Nonnull final QueryableDataSource dataSource,
      @Nonnull final Path jobDirPath,
      @Nonnull final String requestId) {
    final AtomicReference<List<String>> fileUrls = new AtomicReference<>(List.of());
    pipeline.execute(
        Objects.requireNonNull(subject.preparedQuery()),
        dataSource,
        requestId,
        result -> fileUrls.set(writeOutput(result, subject.name(), request, jobDirPath)));
    return fileUrls.get();
  }

  /** Writes a result in the job's format via the shared file writer. */
  @Nonnull
  private List<String> writeOutput(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String name,
      @Nonnull final SqlExportRequest request,
      @Nonnull final Path jobDirPath) {
    return switch (request.format()) {
      case NDJSON -> fileWriter.writeNdjson(result, name, jobDirPath);
      case CSV -> fileWriter.writeCsv(result, name, request.includeHeader(), jobDirPath);
      case PARQUET -> {
        // Reject unresolved (VOID) columns before writing, since Spark's Parquet writer would
        // otherwise fail the job with an opaque internal error.
        ParquetSchemaValidator.validateSchemaForParquet(result.schema());
        yield fileWriter.writeParquet(result, name, jobDirPath);
      }
    };
  }
}
