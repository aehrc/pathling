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

package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Provides the download endpoint for bulk export operation results.
 *
 * @author Felix Naumann
 */
@Component
@Slf4j
public class ExportResultProvider {

  @Nonnull private final ExportResultRegistry exportResultRegistry;

  @Nonnull private final String databasePath;

  @Nonnull private final ServerConfiguration configuration;

  /**
   * Creates a new instance of the export result provider.
   *
   * @param exportResultRegistry the registry for tracking export results
   * @param databasePath the path to the database storage location
   * @param configuration the server configuration for cache settings
   */
  @Autowired
  public ExportResultProvider(
      @Nonnull final ExportResultRegistry exportResultRegistry,
      @Nonnull @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath,
      @Nonnull final ServerConfiguration configuration) {
    this.exportResultRegistry = exportResultRegistry;
    this.databasePath = databasePath;
    this.configuration = configuration;
  }

  /**
   * Enables the download of the result of an export operation.
   *
   * @param jobId the job ID of the export request
   * @param file the name of the file to download
   * @param response the {@link HttpServletResponse} for sending the file content
   */
  @SuppressWarnings({"unused", "TypeMayBeWeakened"})
  @OperationAccess({"export", "bulk-submit"})
  @Operation(name = "$result", idempotent = true, manualResponse = true)
  public void result(
      @Nonnull @OperationParam(name = "job") final String jobId,
      @Nonnull @OperationParam(name = "file") final String file,
      @Nullable final HttpServletResponse response) {
    requireNonNull(response);

    // Set cache headers for async endpoint responses using TTL-based caching.
    final int maxAge = configuration.getAsync().getCacheMaxAge();
    response.setHeader("Cache-Control", "max-age=" + maxAge);

    log.info("Retrieving export result: {}", jobId);

    final ExportResult exportResult = exportResultRegistry.get(jobId);
    if (exportResult == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      throw new ResourceNotFoundError("Unknown job id.");
    }
    final Optional<String> ownerId = exportResult.ownerId();

    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownerId.equals(currentUserId)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'."
              .formatted(currentUserId.orElse("null")));
    }

    final Path requestedFilepath =
        new Path(
            URI.create(databasePath).getPath()
                + Path.SEPARATOR
                + "jobs"
                + Path.SEPARATOR
                + jobId
                + Path.SEPARATOR
                + file);
    final Resource resource = new FileSystemResource(requestedFilepath.toString());

    if (!resource.exists() || !resource.isFile()) {
      throw new ResourceNotFoundError(
          "File '%s' does not exist or is not a file.".formatted(requestedFilepath));
    }

    response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file + "\"");
    response.setContentType("application/octet-stream");
    response.setStatus(HttpServletResponse.SC_OK);
    try (final InputStream inputStream = new FileInputStream(resource.getFile());
        final OutputStream outputStream = response.getOutputStream()) {

      inputStream.transferTo(outputStream);
      outputStream.flush();

    } catch (final IOException e) {
      throw new InternalErrorException("Failed transferring file.", e);
    }
  }
}
