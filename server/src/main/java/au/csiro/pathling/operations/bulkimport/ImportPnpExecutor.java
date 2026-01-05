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

package au.csiro.pathling.operations.bulkimport;

import au.csiro.fhir.auth.AuthConfig;
import au.csiro.fhir.export.BulkExportClient;
import au.csiro.pathling.config.PnpConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.stereotype.Component;

/**
 * Encapsulates the execution of a ping and pull import operation.
 *
 * @author John Grimes
 * @see <a href="https://github.com/smart-on-fhir/bulk-import/blob/master/import-pnp.md">Bulk Data
 *     Import - Ping and Pull Approach</a>
 */
@Component
@Slf4j
public class ImportPnpExecutor {

  // Pattern to match resource type with optional qualifier, e.g. "Patient.0000" -> "Patient"
  private static final Pattern BASE_NAME_WITH_QUALIFIER =
      Pattern.compile("^([A-Za-z]+)(\\.[^.]+)?$");

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final ImportExecutor importExecutor;

  /**
   * Constructor for ImportPnpExecutor.
   *
   * @param serverConfiguration the server configuration
   * @param importExecutor the import executor for processing downloaded files
   */
  public ImportPnpExecutor(
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final ImportExecutor importExecutor) {
    this.serverConfiguration = serverConfiguration;
    this.importExecutor = importExecutor;
  }

  /**
   * Executes the ping and pull import operation.
   *
   * @param pnpRequest the ping and pull import request
   * @param jobId the job identifier for tracking this import operation
   * @return the import response containing details of the imported data
   */
  @Nonnull
  public ImportResponse execute(@Nonnull final ImportPnpRequest pnpRequest, final String jobId) {
    log.info(
        "Received $import-pnp request for exportUrl: {}, exportType: {}",
        pnpRequest.exportUrl(),
        pnpRequest.exportType());

    Path tempDir = null;
    try {
      // Create directory for downloaded files with job ID for uniqueness.
      final PnpConfiguration pnpConfig =
          serverConfiguration.getImport() != null ? serverConfiguration.getImport().getPnp() : null;
      if (pnpConfig == null) {
        throw new InvalidUserInputError("PnP configuration is missing");
      }

      final String downloadLocation =
          pnpConfig.getDownloadLocation() != null
              ? pnpConfig.getDownloadLocation()
              : "/usr/share/staging/pnp";
      final Path baseDir = Path.of(downloadLocation);

      // Ensure base directory exists.
      if (!Files.exists(baseDir)) {
        Files.createDirectories(baseDir);
        log.debug("Created base download directory: {}", baseDir);
      }

      // Create job-specific subdirectory.
      final String tempDirName = "pathling-pnp-import-" + jobId;
      tempDir = baseDir.resolve(tempDirName);
      if (!Files.exists(tempDir)) {
        Files.createDirectories(tempDir);
      }
      log.debug("Using download directory: {}", tempDir);

      // Clean any existing content in the temp directory (in case of retry).
      try (final var paths = Files.walk(tempDir)) {
        paths
            .filter(Files::isRegularFile)
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (final IOException e) {
                    log.warn("Failed to delete existing file in temp directory: {}", path, e);
                  }
                });
      }

      // Determine file extension to filter for.
      final String fileExtension =
          pnpConfig.getFileExtension() != null ? pnpConfig.getFileExtension() : ".ndjson";

      // Download files using fhir-bulk-java.
      final Map<String, Collection<String>> downloadedFiles =
          downloadFiles(pnpRequest, pnpConfig, tempDir, fileExtension);

      // Create an ImportRequest from the downloaded files.
      final ImportRequest importRequest =
          new ImportRequest(
              pnpRequest.originalRequest(),
              downloadedFiles,
              pnpRequest.saveMode(),
              pnpRequest.importFormat());

      // Execute the import using the existing ImportExecutor.
      final ImportResponse response = importExecutor.execute(importRequest, jobId);

      log.info("Ping and pull import completed successfully");
      return response;

    } catch (final IOException e) {
      log.error("Failed to create temporary directory for ping and pull import", e);
      throw new InvalidUserInputError("Failed to create temporary directory: " + e.getMessage(), e);
    } catch (final Exception e) {
      log.error("Ping and pull import failed", e);
      throw new InvalidUserInputError("Ping and pull import failed: " + e.getMessage(), e);
    } finally {
      // Clean up temporary directory.
      if (tempDir != null) {
        cleanupTempDirectory(tempDir);
      }
    }
  }

  private Map<String, Collection<String>> downloadFiles(
      final ImportPnpRequest pnpRequest,
      final PnpConfiguration pnpConfig,
      final Path tempDir,
      final String fileExtension)
      throws Exception {

    // Build the BulkExportClient based on export type.
    // Note: Static mode (manifest-based) is not directly supported by the current API,
    // so we only support dynamic mode for now.
    if ("static".equals(pnpRequest.exportType())) {
      throw new InvalidUserInputError(
          "Static export type is not currently supported. Please use dynamic mode.");
    }

    // Build authentication configuration if client ID is present.
    AuthConfig authConfig = null;
    if (pnpConfig.getClientId() != null && !pnpConfig.getClientId().isBlank()) {
      final var authBuilder = AuthConfig.builder().enabled(true).clientId(pnpConfig.getClientId());

      // Set authentication method (asymmetric or symmetric).
      if (pnpConfig.getPrivateKeyJwk() != null && !pnpConfig.getPrivateKeyJwk().isBlank()) {
        authBuilder.useSMART(true).privateKeyJWK(pnpConfig.getPrivateKeyJwk());
      } else if (pnpConfig.getClientSecret() != null && !pnpConfig.getClientSecret().isBlank()) {
        authBuilder.useSMART(false).clientSecret(pnpConfig.getClientSecret());
        // Set token endpoint if provided.
        if (pnpConfig.getTokenEndpoint() != null) {
          authBuilder.tokenEndpoint(pnpConfig.getTokenEndpoint());
        }
      }

      if (pnpConfig.getScope() != null) {
        authBuilder.scope(pnpConfig.getScope());
      }

      if (pnpConfig.getTokenExpiryTolerance() != null) {
        authBuilder.tokenExpiryTolerance(pnpConfig.getTokenExpiryTolerance());
      }

      authConfig = authBuilder.build();
    }

    // Build the client.
    // Note: fhir-bulk-java creates the output directory, so we pass the parent and a subdirectory
    // name.
    final Path outputDir = tempDir.resolve("export-output");
    final var clientBuilder =
        BulkExportClient.systemBuilder()
            .withFhirEndpointUrl(pnpRequest.exportUrl())
            .withOutputDir(outputDir.toString());

    if (authConfig != null) {
      clientBuilder.withAuthConfig(authConfig);
    }

    final BulkExportClient client = clientBuilder.build();

    // Execute the export and wait for completion.
    log.info("Starting bulk export download from: {}", pnpRequest.exportUrl());
    client.export();
    log.info("Bulk export download completed");

    // Scan the output directory to find downloaded files and organise by resource type.
    return organiseDownloadedFiles(outputDir, fileExtension);
  }

  /**
   * Scans the downloaded files and organises them by resource type extracted from filenames.
   *
   * @param downloadDir the directory containing downloaded files
   * @param fileExtension the file extension to filter for
   * @return a map of resource type to file URLs
   */
  private Map<String, Collection<String>> organiseDownloadedFiles(
      final Path downloadDir, final String fileExtension) throws IOException {
    final Map<String, Collection<String>> result = new HashMap<>();

    // Walk through the download directory to find all files with the specified extension.
    try (final var paths = Files.walk(downloadDir)) {
      paths
          .filter(Files::isRegularFile)
          .filter(path -> path.toString().endsWith(fileExtension))
          .forEach(
              path -> {
                // Get the full filename (with extension)
                final String fileName = path.getFileName().toString();
                // Extract the base name (without extension) for resource type matching
                final String baseName = FilenameUtils.getBaseName(fileName);

                // Extract resource type using the same pattern as FileSource
                final Matcher matcher = BASE_NAME_WITH_QUALIFIER.matcher(baseName);
                if (matcher.matches()) {
                  // Group 1 contains the resource type (e.g., "Patient" from "Patient.0000")
                  final String resourceType = matcher.group(1);

                  // Convert file path to file:// URL for ImportExecutor
                  final String fileUrl = path.toUri().toString();

                  // Add to map keyed by resource type
                  result.computeIfAbsent(resourceType, k -> new ArrayList<>()).add(fileUrl);
                  log.debug("Found {} file: {}", resourceType, fileName);
                } else {
                  log.warn("Could not extract resource type from filename: {}", fileName);
                }
              });
    }

    if (result.isEmpty()) {
      throw new InvalidUserInputError(
          "No files with extension " + fileExtension + " were downloaded");
    }

    log.info("Organised downloaded files by resource type: {}", result.keySet());
    return result;
  }

  /**
   * Recursively deletes the temporary directory and all its contents.
   *
   * @param tempDir the temporary directory to delete
   */
  private void cleanupTempDirectory(final Path tempDir) {
    try {
      if (Files.exists(tempDir)) {
        try (final var paths = Files.walk(tempDir)) {
          paths
              .sorted(Comparator.reverseOrder())
              .forEach(
                  path -> {
                    try {
                      Files.delete(path);
                    } catch (final IOException e) {
                      log.warn("Failed to delete temporary file: {}", path, e);
                    }
                  });
        }
        log.debug("Cleaned up temporary directory: {}", tempDir);
      }
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary directory: {}", tempDir, e);
    }
  }
}
