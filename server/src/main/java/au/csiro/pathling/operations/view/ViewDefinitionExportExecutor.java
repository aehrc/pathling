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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.export.ExportDataSourceBuilder;
import au.csiro.pathling.operations.export.ExportFileWriter;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Executes ViewDefinition queries and writes the results to files, reusing the shared {@link
 * ExportDataSourceBuilder} (filtering) and {@link ExportFileWriter} (job directory and file
 * writing) that back both export operations.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ViewDefinitionExportExecutor {

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final ExportDataSourceBuilder dataSourceBuilder;

  @Nonnull private final ExportFileWriter fileWriter;

  /**
   * Constructs a new ViewDefinitionExportExecutor.
   *
   * @param deltaLake the queryable data source
   * @param fhirContext the FHIR context
   * @param serverConfiguration the server configuration
   * @param dataSourceBuilder the shared export data-source builder (applies filters)
   * @param fileWriter the shared export file writer (job directory and file writing)
   */
  @Autowired
  public ViewDefinitionExportExecutor(
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final ExportDataSourceBuilder dataSourceBuilder,
      @Nonnull final ExportFileWriter fileWriter) {
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
    this.dataSourceBuilder = dataSourceBuilder;
    this.fileWriter = fileWriter;
  }

  /**
   * Executes the view export request and writes the results to files.
   *
   * @param request the export request
   * @param jobId the job ID for this export
   * @return a list of outputs, one per view
   */
  @Nonnull
  public List<ViewExportOutput> execute(
      @Nonnull final ViewDefinitionExportRequest request, @Nonnull final String jobId) {

    final Path jobDirPath = fileWriter.createJobDirectory(jobId);
    final QueryableDataSource dataSource =
        dataSourceBuilder.build(deltaLake, request.since(), request.patientIds());
    final List<ViewExportOutput> outputs = new ArrayList<>();
    final Set<String> usedNames = new HashSet<>();

    for (int i = 0; i < request.views().size(); i++) {
      final ViewInput viewInput = request.views().get(i);
      final String viewName = fileWriter.uniqueName(viewInput.getEffectiveName(i), usedNames);
      usedNames.add(viewName);

      final ViewExportOutput output =
          executeView(viewInput.view(), viewName, request, dataSource, jobDirPath);
      outputs.add(output);
    }

    return outputs;
  }

  /** Executes a single view and writes the results to files. */
  @Nonnull
  private ViewExportOutput executeView(
      @Nonnull final FhirView view,
      @Nonnull final String viewName,
      @Nonnull final ViewDefinitionExportRequest request,
      @Nonnull final QueryableDataSource dataSource,
      @Nonnull final Path jobDirPath) {

    // Execute the view query against the filtered data source.
    final FhirViewExecutor executor =
        new FhirViewExecutor(fhirContext, dataSource, serverConfiguration.getQuery());

    final Dataset<Row> result;
    try {
      result = executor.buildQuery(view);
    } catch (final ConstraintViolationException e) {
      throw new InvalidRequestException(
          "Invalid ViewDefinition '%s': %s".formatted(viewName, e.getMessage()));
    }

    // Write the output in the requested format, reusing the shared file writer.
    final List<String> fileUrls =
        writeOutput(result, viewName, request.format(), request.includeHeader(), jobDirPath);

    return new ViewExportOutput(viewName, fileUrls);
  }

  /** Writes the query result in the requested format via the shared file writer. */
  @Nonnull
  private List<String> writeOutput(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String viewName,
      @Nonnull final ViewExportFormat format,
      final boolean includeHeader,
      @Nonnull final Path jobDirPath) {
    return switch (format) {
      case NDJSON -> fileWriter.writeNdjson(result, viewName, jobDirPath);
      case CSV -> fileWriter.writeCsv(result, viewName, includeHeader, jobDirPath);
      case PARQUET -> fileWriter.writeParquet(result, viewName, jobDirPath);
    };
  }
}
