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

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintViolationException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Executes ViewDefinition queries and writes the results to files.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ViewDefinitionExportExecutor {

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final String databasePath;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final PatientCompartmentService patientCompartmentService;

  /**
   * Constructs a new ViewDefinitionExportExecutor.
   *
   * @param deltaLake the queryable data source
   * @param fhirContext the FHIR context
   * @param sparkSession the Spark session
   * @param databasePath the database path
   * @param serverConfiguration the server configuration
   * @param patientCompartmentService the patient compartment service
   */
  @Autowired
  public ViewDefinitionExportExecutor(
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession,
      @Nonnull @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final PatientCompartmentService patientCompartmentService) {
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.databasePath = databasePath;
    this.serverConfiguration = serverConfiguration;
    this.patientCompartmentService = patientCompartmentService;
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

    final Path jobDirPath = createJobDirectory(jobId);
    final List<ViewExportOutput> outputs = new ArrayList<>();
    final Set<String> usedNames = new HashSet<>();

    for (int i = 0; i < request.views().size(); i++) {
      final ViewInput viewInput = request.views().get(i);
      final String viewName = getUniqueViewName(viewInput.getEffectiveName(i), usedNames);
      usedNames.add(viewName);

      final ViewExportOutput output = executeView(viewInput.view(), viewName, request, jobDirPath);
      outputs.add(output);
    }

    return outputs;
  }

  /** Creates the job directory for storing output files. */
  @Nonnull
  private Path createJobDirectory(@Nonnull final String jobId) {
    final URI warehouseUri = URI.create(databasePath);
    final Path warehousePath = new Path(warehouseUri);
    final Path jobDirPath = new Path(new Path(warehousePath, "jobs"), jobId);
    final Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();

    try {
      final FileSystem fs = FileSystem.get(configuration);
      if (!fs.exists(jobDirPath)) {
        final boolean created = fs.mkdirs(jobDirPath);
        if (!created) {
          throw new InternalErrorException(
              "Failed to create subdirectory at %s for job %s.".formatted(databasePath, jobId));
        }
        log.debug("Created dir {}", jobDirPath);
      }
    } catch (final IOException e) {
      throw new InternalErrorException(
          "Failed to create subdirectory at %s for job %s.".formatted(databasePath, jobId));
    }

    return jobDirPath;
  }

  /** Generates a unique name for a view, avoiding collisions with already-used names. */
  @Nonnull
  private String getUniqueViewName(
      @Nonnull final String baseName, @Nonnull final Set<String> usedNames) {
    if (!usedNames.contains(baseName)) {
      return baseName;
    }
    int suffix = 1;
    while (usedNames.contains(baseName + "_" + suffix)) {
      suffix++;
    }
    return baseName + "_" + suffix;
  }

  /** Executes a single view and writes the results to files. */
  @Nonnull
  private ViewExportOutput executeView(
      @Nonnull final FhirView view,
      @Nonnull final String viewName,
      @Nonnull final ViewDefinitionExportRequest request,
      @Nonnull final Path jobDirPath) {

    // Build the data source with filters applied.
    final QueryableDataSource dataSource = buildDataSource(request);

    // Execute the view query.
    final FhirViewExecutor executor =
        new FhirViewExecutor(fhirContext, dataSource, serverConfiguration.getQuery());

    final Dataset<Row> result;
    try {
      result = executor.buildQuery(view);
    } catch (final ConstraintViolationException e) {
      throw new InvalidRequestException(
          "Invalid ViewDefinition '%s': %s".formatted(viewName, e.getMessage()));
    }

    // Write output based on format.
    final List<String> fileUrls =
        writeOutput(result, viewName, request.format(), request.includeHeader(), jobDirPath);

    return new ViewExportOutput(viewName, fileUrls);
  }

  /** Builds the data source with filters applied. */
  @Nonnull
  private QueryableDataSource buildDataSource(@Nonnull final ViewDefinitionExportRequest request) {
    QueryableDataSource dataSource = deltaLake;

    // Apply _since filter.
    if (request.since() != null) {
      dataSource =
          dataSource.map(
              rowDataset ->
                  rowDataset.filter(
                      "meta.lastUpdated IS NULL OR meta.lastUpdated >= '"
                          + request.since().getValueAsString()
                          + "'"));
    }

    // Apply patient compartment filter if patient IDs were specified.
    if (!request.patientIds().isEmpty()) {
      dataSource = applyPatientCompartmentFilter(dataSource, request.patientIds());
    }

    return dataSource;
  }

  /** Applies patient compartment filter to the data source. */
  @Nonnull
  private QueryableDataSource applyPatientCompartmentFilter(
      @Nonnull final QueryableDataSource dataSource, @Nonnull final Set<String> patientIds) {

    // Filter out resource types that are not in the Patient compartment.
    final QueryableDataSource filtered =
        dataSource.filterByResourceType(patientCompartmentService::isInPatientCompartment);

    // Apply row-level filtering based on patient compartment membership.
    return filtered.map(
        (resourceType, rowDataset) -> {
          final Column patientFilter =
              patientCompartmentService.buildPatientFilter(resourceType, patientIds);
          log.debug(
              "Applying patient compartment filter for resource type {}: {}",
              resourceType,
              patientFilter);
          return rowDataset.filter(patientFilter);
        });
  }

  /** Writes the query result to files in the specified format. */
  @Nonnull
  private List<String> writeOutput(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String viewName,
      @Nonnull final ViewExportFormat format,
      final boolean includeHeader,
      @Nonnull final Path jobDirPath) {

    final String outputPath =
        safelyJoinPaths(jobDirPath.toString(), viewName + format.getFileExtension());

    switch (format) {
      case NDJSON -> {
        return writeNdjson(result, outputPath, viewName, jobDirPath);
      }
      case CSV -> {
        return writeCsv(result, outputPath, viewName, includeHeader, jobDirPath);
      }
      case PARQUET -> {
        return writeParquet(result, outputPath, viewName, jobDirPath);
      }
      default -> throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /** Writes the result as NDJSON files. */
  @Nonnull
  private List<String> writeNdjson(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String outputPath,
      @Nonnull final String viewName,
      @Nonnull final Path jobDirPath) {

    result.write().mode(SaveMode.Overwrite).json(outputPath);

    // Rename partitioned files to follow naming convention.
    return new ArrayList<>(
        FileSystemPersistence.renamePartitionedFiles(sparkSession, outputPath, outputPath, "json"));
  }

  /**
   * Writes the result as CSV files.
   *
   * @throws InvalidRequestException if the dataset contains data types that are not supported by
   *     the CSV format
   */
  @Nonnull
  private List<String> writeCsv(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String outputPath,
      @Nonnull final String viewName,
      final boolean includeHeader,
      @Nonnull final Path jobDirPath) {

    try {
      result.write().mode(SaveMode.Overwrite).option("header", includeHeader).csv(outputPath);
    } catch (final Exception e) {
      // Spark throws AnalysisException when it encounters unsupported data types for a datasource.
      // We convert this to an InvalidRequestException to return a 400 status code.
      if (e instanceof final AnalysisException ae
          && "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE".equals(ae.getErrorClass())) {
        throw new InvalidRequestException(
            "CSV export failed for view '%s': %s".formatted(viewName, e.getMessage()));
      }
      // Re-throw unexpected exceptions as runtime exceptions.
      if (e instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(e);
    }

    // Rename partitioned files to follow naming convention.
    return new ArrayList<>(
        FileSystemPersistence.renamePartitionedFiles(sparkSession, outputPath, outputPath, "csv"));
  }

  /** Writes the result as Parquet files. */
  @Nonnull
  private List<String> writeParquet(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String outputPath,
      @Nonnull final String viewName,
      @Nonnull final Path jobDirPath) {

    result.write().mode(SaveMode.Overwrite).parquet(outputPath);

    // Rename partitioned files to follow the numbered naming convention.
    return new ArrayList<>(
        FileSystemPersistence.renamePartitionedFiles(
            sparkSession, outputPath, outputPath, "parquet"));
  }
}
