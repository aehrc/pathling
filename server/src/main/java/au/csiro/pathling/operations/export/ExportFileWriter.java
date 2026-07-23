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

package au.csiro.pathling.operations.export;

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.io.JobDirectoryFileSystem;
import au.csiro.pathling.library.io.FileSystemPersistence;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Writes asynchronous-export result datasets to files under the per-job directory in the warehouse,
 * and serves the per-job directory and unique-naming helpers. Shared by both {@code
 * $viewdefinition-export} and {@code $sqlquery-export} so that the two operations write their
 * outputs identically (the same directory layout, partition renaming, and CSV unsupported-type
 * handling).
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ExportFileWriter {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final JobDirectoryFileSystem jobDirectoryFileSystem;

  /**
   * Constructs a new ExportFileWriter.
   *
   * @param sparkSession the Spark session used to write the output files
   * @param jobDirectoryFileSystem the shared helper that resolves, creates, and deletes per-job
   *     directories on the warehouse filesystem
   */
  @Autowired
  public ExportFileWriter(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final JobDirectoryFileSystem jobDirectoryFileSystem) {
    this.sparkSession = sparkSession;
    this.jobDirectoryFileSystem = jobDirectoryFileSystem;
  }

  /**
   * Creates the per-job directory under the warehouse for storing output files. The directory is
   * created on the warehouse filesystem, resolved from the warehouse URI, so it works for any
   * warehouse scheme.
   *
   * @param jobId the job id
   * @return the qualified job directory path
   */
  @Nonnull
  public Path createJobDirectory(@Nonnull final String jobId) {
    try {
      jobDirectoryFileSystem.ensureJobDirectory(jobId);
      return jobDirectoryFileSystem.jobDirectory(jobId);
    } catch (final IOException e) {
      throw new InternalErrorException(
          "Failed to create job directory for job %s.".formatted(jobId), e);
    }
  }

  /**
   * Deletes the per-job directory and all its contents, used to clean up partial outputs when an
   * export fails. Failures to delete are logged and swallowed, since cleanup is best-effort.
   *
   * @param jobId the job id whose directory should be removed
   */
  public void deleteJobDirectory(@Nonnull final String jobId) {
    try {
      jobDirectoryFileSystem.deleteJobDirectory(jobId);
      log.debug("Deleted partial output dir for job {}", jobId);
    } catch (final IOException e) {
      log.warn("Failed to delete partial output directory for job {}", jobId, e);
    }
  }

  /**
   * Returns a name unique among the already-used names, appending a numeric suffix on collision.
   *
   * @param baseName the desired base name
   * @param usedNames the names already used in this export (not modified)
   * @return a unique name
   */
  @Nonnull
  public String uniqueName(@Nonnull final String baseName, @Nonnull final Set<String> usedNames) {
    if (!usedNames.contains(baseName)) {
      return baseName;
    }
    int suffix = 1;
    while (usedNames.contains(baseName + "_" + suffix)) {
      suffix++;
    }
    return baseName + "_" + suffix;
  }

  /**
   * Writes the result as NDJSON files, returning the resulting file URLs.
   *
   * @param result the result dataset
   * @param name the output name (used as the file/directory base name)
   * @param jobDirPath the per-job directory
   * @return the written file URLs, one per partition
   */
  @Nonnull
  public List<String> writeNdjson(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String name,
      @Nonnull final Path jobDirPath) {
    final String outputPath = safelyJoinPaths(jobDirPath.toString(), name + ".ndjson");
    result.write().mode(SaveMode.Overwrite).json(outputPath);
    return new ArrayList<>(
        FileSystemPersistence.renamePartitionedFiles(sparkSession, outputPath, outputPath, "json"));
  }

  /**
   * Writes the result as CSV files, returning the resulting file URLs.
   *
   * @param result the result dataset
   * @param name the output name (used as the file/directory base name)
   * @param includeHeader whether to include a CSV header row
   * @param jobDirPath the per-job directory
   * @return the written file URLs, one per partition
   * @throws InvalidRequestException if the dataset contains data types unsupported by CSV
   */
  @Nonnull
  public List<String> writeCsv(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String name,
      final boolean includeHeader,
      @Nonnull final Path jobDirPath) {
    final String outputPath = safelyJoinPaths(jobDirPath.toString(), name + ".csv");
    try {
      result.write().mode(SaveMode.Overwrite).option("header", includeHeader).csv(outputPath);
    } catch (final Exception e) {
      // Spark throws AnalysisException when it encounters unsupported data types for a datasource.
      // We convert this to an InvalidRequestException to return a 400 status code.
      if (e instanceof final AnalysisException ae
          && "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE".equals(ae.getErrorClass())) {
        throw new InvalidRequestException(
            "CSV export failed for output '%s': %s".formatted(name, e.getMessage()));
      }
      if (e instanceof final RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(e);
    }
    return new ArrayList<>(
        FileSystemPersistence.renamePartitionedFiles(sparkSession, outputPath, outputPath, "csv"));
  }

  /**
   * Writes the result as Parquet files, returning the resulting file URLs.
   *
   * @param result the result dataset
   * @param name the output name (used as the file/directory base name)
   * @param jobDirPath the per-job directory
   * @return the written file URLs, one per partition
   */
  @Nonnull
  public List<String> writeParquet(
      @Nonnull final Dataset<Row> result,
      @Nonnull final String name,
      @Nonnull final Path jobDirPath) {
    final String outputPath = safelyJoinPaths(jobDirPath.toString(), name + ".parquet");
    result.write().mode(SaveMode.Overwrite).parquet(outputPath);
    return new ArrayList<>(
        FileSystemPersistence.renamePartitionedFiles(
            sparkSession, outputPath, outputPath, "parquet"));
  }
}
