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
import au.csiro.fhir.export.BulkExportResult;
import au.csiro.fhir.export.BulkExportResult.FileResult;
import au.csiro.filestore.hdfs.HdfsFileStoreFactory;
import au.csiro.pathling.config.PnpConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Encapsulates the execution of a ping and pull import operation.
 *
 * <p>All staging file IO is performed through the Hadoop {@link FileSystem} API so that the same
 * code path works for {@code file://}, {@code s3a://}, {@code hdfs://} and any other
 * Spark-supported scheme. fhir-bulk-java is configured with {@link HdfsFileStoreFactory} so that
 * its own writes use the same scheme handlers.
 *
 * @author John Grimes
 * @see <a href="https://github.com/smart-on-fhir/bulk-import/blob/master/import-pnp.md">Bulk Data
 *     Import - Ping and Pull Approach</a>
 */
@Component
@Slf4j
public class ImportPnpExecutor {

  // Pattern to match resource type with optional qualifier, e.g. "Patient.0000" -> "Patient".
  private static final Pattern BASE_NAME_WITH_QUALIFIER =
      Pattern.compile("^([A-Za-z]+)(\\.[^.]+)?$");

  @Nonnull private static final String DEFAULT_STAGING_SUBDIRECTORY = "staging/pnp";

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final ImportExecutor importExecutor;

  @Nonnull private final Configuration hadoopConfiguration;

  @Nonnull private final String databasePath;

  /**
   * Constructor for ImportPnpExecutor.
   *
   * @param serverConfiguration the server configuration
   * @param importExecutor the import executor for processing downloaded files
   * @param hadoopConfiguration the Hadoop configuration used to access the staging file system
   * @param databasePath the warehouse + database URL, used to derive the default staging location
   */
  public ImportPnpExecutor(
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final ImportExecutor importExecutor,
      @Nonnull final Configuration hadoopConfiguration,
      @Nonnull @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath) {
    this.serverConfiguration = serverConfiguration;
    this.importExecutor = importExecutor;
    this.hadoopConfiguration = hadoopConfiguration;
    this.databasePath = databasePath;
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
    FileSystem fs = null;
    try {
      // Resolve effective configuration.
      PnpConfiguration pnpConfig =
          serverConfiguration.getImport() != null ? serverConfiguration.getImport().getPnp() : null;
      if (pnpConfig == null) {
        pnpConfig = new PnpConfiguration();
      }

      final String stagingBaseUri = resolveStagingBaseUri(pnpConfig);
      final URI stagingBase = URI.create(stagingBaseUri);
      fs = FileSystem.get(stagingBase, hadoopConfiguration);

      final Path baseDir = fs.makeQualified(new Path(stagingBase));
      if (!fs.exists(baseDir) && !fs.mkdirs(baseDir)) {
        throw new IOException("Failed to create base download directory: " + baseDir);
      }

      // Create job-specific subdirectory.
      final String tempDirName = "pathling-pnp-import-" + jobId;
      tempDir = new Path(baseDir, tempDirName);
      if (!fs.exists(tempDir) && !fs.mkdirs(tempDir)) {
        throw new IOException("Failed to create temporary directory: " + tempDir);
      }
      log.debug("Using download directory: {}", tempDir);

      // Clean any existing content in the temp directory (in case of retry).
      cleanRegularFiles(fs, tempDir);

      // Determine file extension from the import format.
      final String fileExtension = "." + pnpRequest.importFormat().getExtension();

      // Download files using fhir-bulk-java.
      final Path outputDir = new Path(tempDir, "export-output");
      final BulkExportClient client = buildBulkExportClient(pnpRequest, pnpConfig, outputDir);
      final Map<String, Collection<String>> downloadedFiles =
          downloadFiles(client, pnpRequest.exportUrl(), outputDir, fs, fileExtension);

      // Create an ImportRequest from the downloaded files.
      final ImportRequest importRequest =
          new ImportRequest(
              pnpRequest.originalRequest(),
              downloadedFiles,
              pnpRequest.saveMode(),
              pnpRequest.importFormat());

      // Execute the import using the existing ImportExecutor with custom allowable sources.
      // This bypasses the configured allowableSources validation for the staging directory,
      // which the server downloaded and trusts. Go via fs.getFileStatus() to obtain a URI in
      // the same canonical form (with empty authority preserved on file://) that fs.listFiles
      // produces for the downloaded files, so the UrlAllowlist string-prefix match holds:
      // tempDir.toUri() alone yields file:/path while listed files come back as file:///path.
      final List<String> pnpAllowableSources =
          List.of(fs.getFileStatus(tempDir).getPath().toUri().toString() + "/");
      final ImportResponse response =
          importExecutor.execute(importRequest, jobId, pnpAllowableSources);

      log.info("Ping and pull import completed successfully");
      return response;

    } catch (final IOException e) {
      log.error("Failed to create temporary directory for ping and pull import", e);
      throw new InvalidUserInputError("Failed to create temporary directory: " + e.getMessage(), e);
    } catch (final Exception e) {
      log.error("Ping and pull import failed", e);
      final String errorMessage = extractRootCauseMessage(e);
      throw new InvalidUserInputError("Ping and pull import failed: " + errorMessage, e);
    } finally {
      // Clean up temporary directory.
      if (tempDir != null && fs != null) {
        cleanupTempDirectory(fs, tempDir);
      }
    }
  }

  /**
   * Resolves the base staging URI for the given configuration.
   *
   * <p>If the configured {@code downloadLocation} carries a scheme it is used verbatim. If it has
   * no scheme it is interpreted as a local filesystem path and rendered as a {@code file://} URI.
   * If unset, the default is a {@code staging/pnp} subdirectory of the warehouse, so non-local
   * deployments do not require a separate persistent volume mount.
   *
   * @param pnpConfig the resolved Ping and Pull configuration
   * @return the base staging URI as a Hadoop-compatible string
   */
  @Nonnull
  String resolveStagingBaseUri(@Nonnull final PnpConfiguration pnpConfig) {
    final String location = pnpConfig.getDownloadLocation();
    if (location == null || location.isBlank()) {
      return databasePath + "/" + DEFAULT_STAGING_SUBDIRECTORY;
    }
    try {
      final URI uri = URI.create(location);
      if (uri.getScheme() != null) {
        return location;
      }
    } catch (final IllegalArgumentException ignored) {
      // Fall through; treat as a local filesystem path.
    }
    return "file://" + location;
  }

  /**
   * Builds a {@link BulkExportClient} configured for the given request and output directory.
   *
   * @param pnpRequest the ping and pull import request
   * @param pnpConfig the PnP configuration
   * @param outputDir the directory where downloaded files will be written
   * @return the configured bulk export client
   */
  @Nonnull
  BulkExportClient buildBulkExportClient(
      @Nonnull final ImportPnpRequest pnpRequest,
      @Nonnull final PnpConfiguration pnpConfig,
      @Nonnull final Path outputDir) {

    // Static mode is not currently supported.
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

    // Build the client. The Hadoop FileStore factory routes writes through the same scheme
    // handlers as the rest of the server, allowing s3a://, hdfs:// and other warehouses.
    final var clientBuilder =
        BulkExportClient.systemBuilder()
            .withFhirEndpointUrl(pnpRequest.exportUrl())
            .withOutputDir(outputDir.toString())
            .withFileStoreFactory(new HdfsFileStoreFactory(hadoopConfiguration));

    if (authConfig != null) {
      clientBuilder.withAuthConfig(authConfig);
    }

    // Pass through Bulk Data Export parameters if provided.
    if (!pnpRequest.types().isEmpty()) {
      clientBuilder.withTypes(pnpRequest.types());
    }
    pnpRequest.since().ifPresent(since -> clientBuilder.withSince(since));
    pnpRequest.until().ifPresent(until -> clientBuilder.withUntil(until));
    // Use the standard Parquet MIME type when requesting Parquet format, as fhir-bulk-java
    // expects this format. For other formats, use the format's native code.
    final String outputFormat =
        pnpRequest.importFormat() == ImportFormat.PARQUET
            ? "application/vnd.apache.parquet"
            : pnpRequest.importFormat().getCode();
    clientBuilder.withOutputFormat(outputFormat);
    clientBuilder.withOutputExtension(pnpRequest.importFormat().getExtension());
    if (!pnpRequest.elements().isEmpty()) {
      clientBuilder.withElements(pnpRequest.elements());
    }
    if (!pnpRequest.typeFilters().isEmpty()) {
      clientBuilder.withTypeFilters(pnpRequest.typeFilters());
    }
    if (!pnpRequest.includeAssociatedData().isEmpty()) {
      clientBuilder.withIncludeAssociatedData(pnpRequest.includeAssociatedData());
    }

    return clientBuilder.build();
  }

  /**
   * Executes the bulk export download, validates that all downloaded files remain within the output
   * directory, and organises the files by resource type.
   *
   * @param client the bulk export client
   * @param exportUrl the original export URL, used to verify manifest URL origin
   * @param outputDir the expected output directory for downloaded files
   * @param fs the file system used to validate and walk the output directory
   * @param fileExtension the file extension to filter for
   * @return a map of resource type to file URLs
   */
  Map<String, Collection<String>> downloadFiles(
      @Nonnull final BulkExportClient client,
      @Nonnull final String exportUrl,
      @Nonnull final Path outputDir,
      @Nonnull final FileSystem fs,
      @Nonnull final String fileExtension)
      throws Exception {

    // Execute the export and wait for completion.
    log.info("Starting bulk export download from: {}", exportUrl);
    final BulkExportResult exportResult = client.export();
    log.info("Bulk export download completed");

    // Reject manifest entries that point to a different origin than the export URL, so that
    // bearer tokens are not forwarded to untrusted hosts.
    validateManifestUrls(exportUrl, exportResult);

    // Reject downloads that ended up outside the expected staging directory.
    validateDownloadResult(exportResult, outputDir, fs);

    // Scan the output directory to find downloaded files and organise by resource type.
    return organiseDownloadedFiles(outputDir, fs, fileExtension);
  }

  /**
   * Validates that all downloaded files in the export result are located within the specified
   * output directory. If any file is found outside the output directory, it is deleted and an
   * {@link InvalidUserInputError} is thrown.
   *
   * <p>For local {@code file://} schemes the canonical (symlink-resolved) path is also checked, to
   * defeat symlink escapes. For non-local schemes (S3, HDFS, etc.) symlinks do not exist or are not
   * honoured by the underlying object store, so the lexical containment check is sufficient.
   *
   * @param exportResult the result of the bulk export operation
   * @param outputDir the allowed output directory
   * @param fs the file system used to qualify and delete paths
   */
  static void validateDownloadResult(
      @Nonnull final BulkExportResult exportResult,
      @Nonnull final Path outputDir,
      @Nonnull final FileSystem fs)
      throws IOException {

    // This check relies on the bulk-export library reporting accurate destination URIs in the
    // result. If the library ever wrote to a path different from the one it reports, this guard
    // would not detect it; the root-cause fix belongs in fhir-bulk-java. This is a compensating
    // control that closes the documented PoC on the assumption that destinations are truthful.
    final Path qualifiedOutputDir = fs.makeQualified(outputDir);
    final String outputDirString = qualifiedOutputDir.toString();
    final String outputPrefix =
        outputDirString.endsWith("/") ? outputDirString : outputDirString + "/";

    for (final FileResult fileResult : exportResult.getResults()) {
      final URI destination = fileResult.getDestination();
      final Path qualifiedDest = fs.makeQualified(new Path(destination));
      if (!qualifiedDest.toString().startsWith(outputPrefix)) {
        // Attempt to delete the escaped file to prevent it from being accessed later.
        try {
          fs.delete(qualifiedDest, false);
        } catch (final IOException e) {
          log.warn("Failed to delete escaped file: {}", qualifiedDest, e);
        }
        throw new InvalidUserInputError(
            "Downloaded file is outside the download directory: " + qualifiedDest);
      }

      if ("file".equals(qualifiedDest.toUri().getScheme())) {
        // Resolve symlinks before re-checking containment.
        try {
          final java.nio.file.Path realDest =
              java.nio.file.Path.of(qualifiedDest.toUri()).toRealPath();
          final java.nio.file.Path realOutputDir =
              java.nio.file.Path.of(qualifiedOutputDir.toUri()).toRealPath();
          if (!realDest.startsWith(realOutputDir)) {
            try {
              fs.delete(qualifiedDest, false);
            } catch (final IOException e) {
              log.warn("Failed to delete escaped file: {}", qualifiedDest, e);
            }
            throw new InvalidUserInputError(
                "Downloaded file is outside the download directory: " + realDest);
          }
        } catch (final IOException e) {
          throw new InvalidUserInputError(
              "Could not validate downloaded file path: " + qualifiedDest, e);
        }
      }
    }
  }

  /**
   * Scans the downloaded files and organises them by resource type extracted from filenames.
   *
   * @param downloadDir the directory containing downloaded files
   * @param fs the file system to use for listing
   * @param fileExtension the file extension to filter for
   * @return a map of resource type to file URLs
   */
  @Nonnull
  private Map<String, Collection<String>> organiseDownloadedFiles(
      @Nonnull final Path downloadDir,
      @Nonnull final FileSystem fs,
      @Nonnull final String fileExtension)
      throws IOException {
    final Map<String, Collection<String>> result = new HashMap<>();

    final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(downloadDir, true);
    while (iterator.hasNext()) {
      final LocatedFileStatus status = iterator.next();
      if (!status.isFile()) {
        continue;
      }
      final Path path = status.getPath();
      final String fileName = path.getName();
      if (!fileName.endsWith(fileExtension)) {
        continue;
      }
      final String baseName = FilenameUtils.getBaseName(fileName);

      // Extract resource type using the same pattern as FileSource.
      final Matcher matcher = BASE_NAME_WITH_QUALIFIER.matcher(baseName);
      if (matcher.matches()) {
        final String resourceType = matcher.group(1);
        final String fileUrl = fs.makeQualified(path).toUri().toString();
        result.computeIfAbsent(resourceType, k -> new ArrayList<>()).add(fileUrl);
        log.debug("Found {} file: {}", resourceType, fileName);
      } else {
        log.warn("Could not extract resource type from filename: {}", fileName);
      }
    }

    if (result.isEmpty()) {
      throw new InvalidUserInputError(
          "No files with extension " + fileExtension + " were downloaded");
    }

    log.info("Organised downloaded files by resource type: {}", result.keySet());
    return result;
  }

  /**
   * Validates that all manifest download URLs share the same origin as the export URL.
   *
   * @param exportUrl the original export URL
   * @param exportResult the bulk export result containing downloaded file sources
   * @throws InvalidUserInputError if any download URL originates from a different host
   */
  static void validateManifestUrls(
      @Nonnull final String exportUrl, @Nonnull final BulkExportResult exportResult) {
    final URI exportUri = URI.create(exportUrl);
    final String exportOrigin = originOf(exportUri);

    for (final FileResult fileResult : exportResult.getResults()) {
      final String sourceOrigin = originOf(fileResult.getSource());
      if (!exportOrigin.equalsIgnoreCase(sourceOrigin)) {
        throw new InvalidUserInputError(
            "Manifest download URL origin does not match export URL origin: "
                + fileResult.getSource()
                + " (expected origin: "
                + exportOrigin
                + ")");
      }
    }
  }

  /**
   * Extracts the origin (scheme + host + port) from a URI.
   *
   * @param uri the URI to extract the origin from
   * @return the origin string in the form scheme://host:port (or scheme://host if default port)
   */
  @Nonnull
  static String originOf(@Nonnull final URI uri) {
    final String scheme = uri.getScheme();
    final String host = uri.getHost();
    final int port = uri.getPort();
    if (scheme == null || host == null) {
      throw new InvalidUserInputError("Invalid URL: " + uri);
    }
    final boolean isDefaultPort =
        ("http".equalsIgnoreCase(scheme) && port == 80)
            || ("https".equalsIgnoreCase(scheme) && port == 443);
    return isDefaultPort || port == -1
        ? scheme.toLowerCase() + "://" + host.toLowerCase()
        : scheme.toLowerCase() + "://" + host.toLowerCase() + ":" + port;
  }

  /**
   * Removes regular files within the directory tree rooted at {@code dir}, leaving subdirectories
   * intact. Used to clean stale content on retry.
   */
  private static void cleanRegularFiles(@Nonnull final FileSystem fs, @Nonnull final Path dir)
      throws IOException {
    if (!fs.exists(dir)) {
      return;
    }
    final RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(dir, true);
    while (iterator.hasNext()) {
      final LocatedFileStatus status = iterator.next();
      if (!status.isFile()) {
        continue;
      }
      try {
        fs.delete(status.getPath(), false);
      } catch (final IOException e) {
        log.warn("Failed to delete existing file in temp directory: {}", status.getPath(), e);
      }
    }
  }

  /**
   * Recursively deletes the temporary directory and all its contents.
   *
   * @param fs the file system on which the temporary directory exists
   * @param tempDir the temporary directory to delete
   */
  private void cleanupTempDirectory(@Nonnull final FileSystem fs, @Nonnull final Path tempDir) {
    try {
      if (fs.exists(tempDir)) {
        fs.delete(tempDir, true);
        log.debug("Cleaned up temporary directory: {}", tempDir);
      }
    } catch (final IOException e) {
      log.warn("Failed to clean up temporary directory: {}", tempDir, e);
    }
  }

  /**
   * Extracts a meaningful error message from an exception, traversing the cause chain if necessary.
   * This handles cases where exceptions are wrapped with null or blank messages (common with Spark
   * and Delta Lake exceptions).
   *
   * @param e the exception to extract the message from
   * @return the first non-blank message found in the cause chain, or the exception class name if no
   *     message is available
   */
  @Nonnull
  static String extractRootCauseMessage(@Nonnull final Throwable e) {
    Throwable current = e;
    while (current != null) {
      final String message = current.getMessage();
      if (message != null && !message.isBlank()) {
        return message;
      }
      current = current.getCause();
    }
    return e.getClass().getSimpleName();
  }
}
