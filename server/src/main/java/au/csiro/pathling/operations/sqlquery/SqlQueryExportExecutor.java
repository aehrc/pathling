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

import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.operations.export.ExportManifestOutput;
import au.csiro.pathling.operations.view.ViewExportFormat;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Runs each query of a {@code $sqlquery-export} request via the shared {@link SqlQueryPipeline} and
 * writes the result of each to files, producing one output per query. Reuses the shared {@link
 * ExportDataSourceBuilder} (filtering) and {@link ExportFileWriter} (job directory and file
 * writing) that back {@code $viewdefinition-export}.
 *
 * <p>Execution is all-or-nothing: if any query fails, the exception propagates and the whole export
 * fails, so no completion manifest is produced.
 *
 * @author John Grimes
 */
@Component
public class SqlQueryExportExecutor {

  @Nonnull private final SqlQueryPipeline pipeline;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final ExportDataSourceBuilder dataSourceBuilder;

  @Nonnull private final ExportFileWriter fileWriter;

  /**
   * Constructs a new SqlQueryExportExecutor.
   *
   * @param pipeline the shared SQL query pipeline (execution)
   * @param deltaLake the queryable data source backing FhirView execution
   * @param dataSourceBuilder the shared export data-source builder (applies filters)
   * @param fileWriter the shared export file writer (job directory and file writing)
   */
  @Autowired
  public SqlQueryExportExecutor(
      @Nonnull final SqlQueryPipeline pipeline,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final ExportDataSourceBuilder dataSourceBuilder,
      @Nonnull final ExportFileWriter fileWriter) {
    this.pipeline = pipeline;
    this.deltaLake = deltaLake;
    this.dataSourceBuilder = dataSourceBuilder;
    this.fileWriter = fileWriter;
  }

  /**
   * Executes the export request and writes the results to files, one output per query.
   *
   * @param request the export request
   * @param jobId the job id for this export
   * @return the outputs, one per query, in order
   */
  @Nonnull
  public List<ExportManifestOutput> execute(
      @Nonnull final SqlQueryExportRequest request, @Nonnull final String jobId) {

    final Path jobDirPath = fileWriter.createJobDirectory(jobId);
    final QueryableDataSource dataSource =
        dataSourceBuilder.build(deltaLake, request.since(), request.patientIds());
    final List<ExportManifestOutput> outputs = new ArrayList<>();
    final Set<String> usedNames = new HashSet<>();

    for (int i = 0; i < request.queries().size(); i++) {
      final QueryInput query = request.queries().get(i);
      final String outputName = fileWriter.uniqueName(query.getEffectiveName(i), usedNames);
      usedNames.add(outputName);

      // Namespace the request-scoped temp views per query within the job.
      final String requestId = jobId + "-" + i;
      final AtomicReference<List<String>> fileUrls = new AtomicReference<>(List.of());
      pipeline.execute(
          query.preparedQuery(),
          dataSource,
          requestId,
          result ->
              fileUrls.set(
                  writeOutput(
                      result, outputName, request.format(), request.includeHeader(), jobDirPath)));

      outputs.add(new ExportManifestOutput(outputName, fileUrls.get()));
    }

    return outputs;
  }

  /** Writes the query result in the requested format via the shared file writer. */
  @Nonnull
  private List<String> writeOutput(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String name,
      @Nonnull final ViewExportFormat format,
      final boolean includeHeader,
      @Nonnull final Path jobDirPath) {
    return switch (format) {
      case NDJSON -> fileWriter.writeNdjson(result, name, jobDirPath);
      case CSV -> fileWriter.writeCsv(result, name, includeHeader, jobDirPath);
      case PARQUET -> fileWriter.writeParquet(result, name, jobDirPath);
    };
  }
}
