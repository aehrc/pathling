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

import au.csiro.pathling.operations.view.ResultStreamingHelper;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

/**
 * Streams a SQL query result to the HTTP response in the requested format. Owns the format dispatch
 * and the Parquet temporary-file lifecycle.
 */
@Slf4j
@Component
public class SqlQueryResultStreamer {

  @Nonnull private final ResultStreamingHelper streamingHelper;

  /** Constructs a new SqlQueryResultStreamer. */
  public SqlQueryResultStreamer() {
    this.streamingHelper = new ResultStreamingHelper(ViewDefinitionGson.create());
  }

  /**
   * Streams the result dataset to the response in the requested format.
   *
   * @param result the result dataset
   * @param outputFormat the requested output format
   * @param includeHeader whether to include a header row when emitting CSV
   * @param response the HTTP response to write to
   */
  public void stream(
      @Nonnull final Dataset<Row> result,
      @Nonnull final SqlQueryOutputFormat outputFormat,
      final boolean includeHeader,
      @Nonnull final HttpServletResponse response) {

    response.setContentType(outputFormat.getContentType());
    if (outputFormat != SqlQueryOutputFormat.PARQUET) {
      response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    }
    response.setStatus(HttpServletResponse.SC_OK);

    try {
      switch (outputFormat) {
        case PARQUET -> streamParquet(result, response);
        case FHIR -> streamFhir(result, response);
        default -> streamTabular(result, outputFormat, includeHeader, response);
      }
    } catch (final IOException e) {
      log.error("Error streaming SQL query results", e);
      throw new InvalidRequestException("Error streaming results: " + e.getMessage());
    }
  }

  private void streamFhir(
      @Nonnull final Dataset<Row> result, @Nonnull final HttpServletResponse response)
      throws IOException {
    final OutputStream outputStream = response.getOutputStream();
    streamingHelper.streamFhirJson(outputStream, result.toLocalIterator(), result.schema());
  }

  private void streamTabular(
      @Nonnull final Dataset<Row> result,
      @Nonnull final SqlQueryOutputFormat outputFormat,
      final boolean includeHeader,
      @Nonnull final HttpServletResponse response)
      throws IOException {

    final OutputStream outputStream = response.getOutputStream();
    final StructType schema = result.schema();

    if (outputFormat == SqlQueryOutputFormat.CSV && includeHeader) {
      final List<String> columnNames = java.util.Arrays.stream(schema.fieldNames()).toList();
      streamingHelper.writeCsvHeader(outputStream, columnNames);
      outputStream.flush();
    }

    final Iterator<Row> iterator = result.toLocalIterator();
    switch (outputFormat) {
      case NDJSON -> streamingHelper.streamNdjson(outputStream, iterator, schema);
      case JSON -> streamingHelper.writeJson(outputStream, iterator, schema);
      default -> streamingHelper.streamCsv(outputStream, iterator, schema);
    }
    outputStream.flush();
  }

  private void streamParquet(
      @Nonnull final Dataset<Row> result, @Nonnull final HttpServletResponse response)
      throws IOException {

    final Path tempDir = Files.createTempDirectory("sqlquery-parquet-");
    final String outputPath = tempDir.resolve("result").toString();

    try {
      result.coalesce(1).write().mode(SaveMode.Overwrite).parquet(outputPath);

      final Path parquetFile = findParquetFile(Path.of(outputPath));
      if (parquetFile == null) {
        throw new InvalidRequestException("Failed to generate Parquet output");
      }

      response.setContentType(SqlQueryOutputFormat.PARQUET.getContentType());
      try (final var inputStream = Files.newInputStream(parquetFile);
          final var outputStream = response.getOutputStream()) {
        inputStream.transferTo(outputStream);
        outputStream.flush();
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  @Nullable
  private Path findParquetFile(@Nonnull final Path directory) throws IOException {
    if (!Files.isDirectory(directory)) {
      return null;
    }
    try (final var stream = Files.list(directory)) {
      return stream.filter(p -> p.toString().endsWith(".parquet")).findFirst().orElse(null);
    }
  }

  private void deleteRecursively(@Nonnull final Path path) {
    try {
      if (Files.isDirectory(path)) {
        try (final var stream = Files.list(path)) {
          stream.forEach(this::deleteRecursively);
        }
      }
      Files.deleteIfExists(path);
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary file: {}", path, e);
    }
  }
}
