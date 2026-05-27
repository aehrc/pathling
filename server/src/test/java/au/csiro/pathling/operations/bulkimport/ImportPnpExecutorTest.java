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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import au.csiro.fhir.export.BulkExportClient;
import au.csiro.fhir.export.BulkExportResult;
import au.csiro.fhir.export.BulkExportResult.FileResult;
import au.csiro.pathling.config.ImportConfiguration;
import au.csiro.pathling.config.PnpConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.io.SaveMode;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for ImportPnpExecutor, focusing on error message extraction from nested exceptions and
 * download location validation.
 *
 * @author John Grimes
 */
@ExtendWith(MockitoExtension.class)
class ImportPnpExecutorTest {

  /** Constructs an executor against a local file system rooted at the given staging directory. */
  private static ImportPnpExecutor newExecutor(
      final ServerConfiguration config,
      final ImportExecutor importExecutor,
      final java.nio.file.Path stagingDir) {
    return new ImportPnpExecutor(
        config, importExecutor, new Configuration(), "file://" + stagingDir.toAbsolutePath());
  }

  private static FileSystem localFileSystem(final java.nio.file.Path tempDir) throws IOException {
    return FileSystem.get(tempDir.toUri(), new Configuration());
  }

  // ========================================
  // Error message extraction tests
  // ========================================

  /**
   * Tests that when an exception is thrown with a null message but a nested cause with a message,
   * the root cause message is extracted. This simulates the Delta Lake error scenario where
   * exceptions are wrapped with intermediate exceptions that have null messages.
   */
  @Test
  void extractsRootCauseMessageWhenExceptionMessageIsNull() {
    // Given - an exception chain where the outermost exception has null message but the nested
    // cause contains the actual error message (simulating Delta/Spark exception wrapping).
    final String rootCauseMessage =
        "[DELTA_PATH_EXISTS] Cannot write to already existent path file:/test/Patient.parquet";
    final RuntimeException rootCause = new RuntimeException(rootCauseMessage);
    final RuntimeException wrappedException = new RuntimeException(null, rootCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(wrappedException);

    // Then - the message should be the root cause message, not "null".
    assertThat(result)
        .contains("DELTA_PATH_EXISTS")
        .contains("Cannot write to already existent path");
  }

  /**
   * Tests that when an exception has a direct message, that message is used rather than traversing
   * the cause chain.
   */
  @Test
  void usesDirectMessageWhenAvailable() {
    // Given - an exception with a direct message.
    final String directMessage = "Direct error message";
    final RuntimeException exception = new RuntimeException(directMessage);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(exception);

    // Then - the message should be the direct message.
    assertThat(result).isEqualTo(directMessage);
  }

  /**
   * Tests that when an exception has multiple levels of nesting, the first non-null message in the
   * cause chain is used.
   */
  @Test
  void extractsFirstNonNullMessageFromDeepCauseChain() {
    // Given - a deeply nested exception chain where only the deepest cause has a message.
    final String deepCauseMessage = "Deep root cause error";
    final RuntimeException deepCause = new RuntimeException(deepCauseMessage);
    final RuntimeException middleCause = new RuntimeException(null, deepCause);
    final RuntimeException outerCause = new RuntimeException(null, middleCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerCause);

    // Then - the message should be the deep cause message.
    assertThat(result).isEqualTo(deepCauseMessage);
  }

  /**
   * Tests that when an exception and all its causes have null messages, the exception class name is
   * used as a fallback.
   */
  @Test
  void usesExceptionClassNameWhenNoMessageAvailable() {
    // Given - an exception chain where all messages are null.
    final RuntimeException innerCause = new RuntimeException((String) null);
    final RuntimeException outerCause = new RuntimeException(null, innerCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerCause);

    // Then - the result should be the exception class name.
    assertThat(result).isEqualTo("RuntimeException");
  }

  /**
   * Tests that blank messages are treated the same as null messages and the cause chain is
   * traversed.
   */
  @Test
  void treatsBlankMessageAsNull() {
    // Given - an exception with a blank message but a cause with a real message.
    final String causeMessage = "Actual error message";
    final RuntimeException cause = new RuntimeException(causeMessage);
    final RuntimeException outerException = new RuntimeException("   ", cause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerException);

    // Then - the result should be the cause message, not blank spaces.
    assertThat(result).isEqualTo(causeMessage);
  }

  /** Tests that empty string messages are treated the same as null. */
  @Test
  void treatsEmptyStringMessageAsNull() {
    // Given - an exception with an empty message but a cause with a real message.
    final String causeMessage = "Nested error";
    final RuntimeException cause = new RuntimeException(causeMessage);
    final RuntimeException outerException = new RuntimeException("", cause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerException);

    // Then - the result should be the cause message.
    assertThat(result).isEqualTo(causeMessage);
  }

  /** Tests that the first non-blank message is used even if deeper causes also have messages. */
  @Test
  void usesFirstNonBlankMessageInChain() {
    // Given - an exception chain with messages at multiple levels.
    final RuntimeException deepCause = new RuntimeException("Deep message");
    final RuntimeException middleCause = new RuntimeException("Middle message", deepCause);
    final RuntimeException outerCause = new RuntimeException(null, middleCause);

    // When
    final String result = ImportPnpExecutor.extractRootCauseMessage(outerCause);

    // Then - the first non-blank message (middle) should be returned.
    assertThat(result).isEqualTo("Middle message");
  }

  // ========================================
  // Download location validation tests
  // ========================================

  /**
   * Tests that when a BulkExportResult contains a file whose destination resolves outside the
   * output directory, validateDownloadResult throws InvalidUserInputError and deletes the escaped
   * file.
   */
  @Test
  void rejectsDownloadedFileOutsideOutputDir(@TempDir final java.nio.file.Path tempDir)
      throws IOException {
    final java.nio.file.Path outputDir = tempDir.resolve("export-output");
    Files.createDirectories(outputDir);
    final java.nio.file.Path escapedFile = tempDir.resolve("escaped.ndjson");
    Files.writeString(escapedFile, "escaped content");

    final BulkExportResult exportResult =
        BulkExportResult.of(
            Instant.now(),
            List.of(
                BulkExportResult.FileResult.of(
                    URI.create("http://example.org/test.ndjson"), escapedFile.toUri(), 100L)));

    final FileSystem fs = localFileSystem(tempDir);
    final Path outputDirPath = new Path(outputDir.toUri());

    assertThatThrownBy(
            () -> ImportPnpExecutor.validateDownloadResult(exportResult, outputDirPath, fs))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("outside the download directory");

    // The escaped file should be deleted.
    assertThat(escapedFile).doesNotExist();
  }

  /**
   * Tests that when a BulkExportResult contains only files within the output directory,
   * validateDownloadResult completes without error.
   */
  @Test
  void acceptsDownloadedFilesInsideOutputDir(@TempDir final java.nio.file.Path tempDir)
      throws IOException {
    final java.nio.file.Path outputDir = tempDir.resolve("export-output");
    Files.createDirectories(outputDir);
    final java.nio.file.Path validFile = outputDir.resolve("Patient.0000.ndjson");
    Files.writeString(validFile, "valid content");

    final BulkExportResult exportResult =
        BulkExportResult.of(
            Instant.now(),
            List.of(
                BulkExportResult.FileResult.of(
                    URI.create("http://example.org/test.ndjson"), validFile.toUri(), 100L)));

    final FileSystem fs = localFileSystem(tempDir);
    final Path outputDirPath = new Path(outputDir.toUri());

    // Should not throw.
    ImportPnpExecutor.validateDownloadResult(exportResult, outputDirPath, fs);
  }

  /**
   * Tests that a symlink whose target lies outside the output directory is rejected, even though
   * the link itself is inside. This exercises the canonical (toRealPath) check applied for the
   * local file:// scheme.
   */
  @Test
  void rejectsSymlinkPointingOutsideOutputDir(@TempDir final java.nio.file.Path tempDir)
      throws IOException {
    final java.nio.file.Path outputDir = tempDir.resolve("export-output");
    Files.createDirectories(outputDir);
    final java.nio.file.Path outside = tempDir.resolve("outside.ndjson");
    Files.writeString(outside, "secret");
    final java.nio.file.Path link = outputDir.resolve("escape.ndjson");
    try {
      Files.createSymbolicLink(link, outside);
    } catch (final UnsupportedOperationException | IOException e) {
      // Symlinks unsupported on this filesystem; skip.
      return;
    }

    final BulkExportResult exportResult =
        BulkExportResult.of(
            Instant.now(),
            List.of(
                BulkExportResult.FileResult.of(
                    URI.create("http://example.org/test.ndjson"), link.toUri(), 100L)));

    final FileSystem fs = localFileSystem(tempDir);
    final Path outputDirPath = new Path(outputDir.toUri());

    assertThatThrownBy(
            () -> ImportPnpExecutor.validateDownloadResult(exportResult, outputDirPath, fs))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("outside the download directory");
  }

  /**
   * Tests that downloadFiles delegates to the client, validates the result, and returns organised
   * files for valid downloads.
   */
  @Test
  void downloadFilesReturnsOrganisedFilesForValidResult(@TempDir final java.nio.file.Path tempDir)
      throws Exception {
    final java.nio.file.Path outputDir = tempDir.resolve("export-output");
    Files.createDirectories(outputDir);
    final java.nio.file.Path patientFile = outputDir.resolve("Patient.0000.ndjson");
    Files.writeString(patientFile, "{}");

    final BulkExportClient mockClient = mock(BulkExportClient.class);
    when(mockClient.export())
        .thenReturn(
            BulkExportResult.of(
                Instant.now(),
                List.of(
                    BulkExportResult.FileResult.of(
                        URI.create("http://example.org/Patient.ndjson"),
                        patientFile.toUri(),
                        2L))));

    final ImportPnpExecutor executor =
        newExecutor(new ServerConfiguration(), mock(ImportExecutor.class), tempDir);
    final FileSystem fs = localFileSystem(tempDir);
    final Path outputDirPath = new Path(outputDir.toUri());

    final var result =
        executor.downloadFiles(mockClient, "http://example.org/fhir", outputDirPath, fs, ".ndjson");

    assertThat(result).containsKey("Patient");
    final String expectedUrl = fs.makeQualified(new Path(patientFile.toUri())).toUri().toString();
    assertThat(result.get("Patient")).containsExactly(expectedUrl);
  }

  /**
   * Tests that when execute() aborts due to a path escape, the temporary directory is still cleaned
   * up in the finally block.
   */
  @Test
  void cleansUpTempDirAfterPathEscape(@TempDir final java.nio.file.Path tempDir) throws Exception {
    final ServerConfiguration config = new ServerConfiguration();
    final PnpConfiguration pnpConfig = new PnpConfiguration();
    pnpConfig.setDownloadLocation(tempDir.toAbsolutePath().toString());
    final ImportConfiguration importConfig = new ImportConfiguration();
    importConfig.setPnp(pnpConfig);
    importConfig.setAllowableSources(java.util.List.of());
    config.setImport(importConfig);

    final java.nio.file.Path escapedFile = tempDir.resolve("escaped.ndjson");
    Files.writeString(escapedFile, "escaped");

    final BulkExportClient mockClient = mock(BulkExportClient.class);
    when(mockClient.export())
        .thenReturn(
            BulkExportResult.of(
                Instant.now(),
                java.util.List.of(
                    BulkExportResult.FileResult.of(
                        URI.create("http://example.org/test.ndjson"), escapedFile.toUri(), 100L))));

    final ImportExecutor mockImportExecutor = mock(ImportExecutor.class);
    final ImportPnpExecutor spyExecutor = spy(newExecutor(config, mockImportExecutor, tempDir));
    doReturn(mockClient).when(spyExecutor).buildBulkExportClient(any(), any(), any());

    final ImportPnpRequest request =
        new ImportPnpRequest(
            "test-url",
            "http://example.org/fhir",
            "dynamic",
            SaveMode.OVERWRITE,
            ImportFormat.NDJSON,
            java.util.List.of(),
            java.util.Optional.empty(),
            java.util.Optional.empty(),
            java.util.List.of(),
            java.util.List.of(),
            java.util.List.of());

    assertThatThrownBy(() -> spyExecutor.execute(request, "testjob"))
        .isInstanceOf(InvalidUserInputError.class);

    // The temp directory should be cleaned up.
    assertThat(tempDir.resolve("pathling-pnp-import-testjob")).doesNotExist();
  }

  /**
   * Tests that downloadFiles propagates the InvalidUserInputError when validateDownloadResult
   * detects an escaped file.
   */
  @Test
  void downloadFilesThrowsWhenResultContainsEscapedFile(@TempDir final java.nio.file.Path tempDir)
      throws Exception {
    final java.nio.file.Path outputDir = tempDir.resolve("export-output");
    Files.createDirectories(outputDir);
    final java.nio.file.Path escapedFile = tempDir.resolve("escaped.ndjson");
    Files.writeString(escapedFile, "escaped content");

    final BulkExportClient mockClient = mock(BulkExportClient.class);
    when(mockClient.export())
        .thenReturn(
            BulkExportResult.of(
                Instant.now(),
                List.of(
                    BulkExportResult.FileResult.of(
                        URI.create("http://example.org/test.ndjson"), escapedFile.toUri(), 100L))));

    final ImportPnpExecutor executor =
        newExecutor(new ServerConfiguration(), mock(ImportExecutor.class), tempDir);
    final FileSystem fs = localFileSystem(tempDir);
    final Path outputDirPath = new Path(outputDir.toUri());

    assertThatThrownBy(
            () ->
                executor.downloadFiles(
                    mockClient, "http://example.org/fhir", outputDirPath, fs, ".ndjson"))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("outside the download directory");
  }

  // ========================================
  // Staging URI resolution tests
  // ========================================

  /**
   * Tests that when no downloadLocation is configured, the staging URI defaults to a subdirectory
   * of the warehouse so that non-local deployments do not require a separate persistent volume.
   */
  @Test
  void defaultsStagingUriToWarehouseSubdirectory() {
    final ImportPnpExecutor executor =
        new ImportPnpExecutor(
            new ServerConfiguration(),
            mock(ImportExecutor.class),
            new Configuration(),
            "s3a://my-bucket/warehouse");
    assertThat(executor.resolveStagingBaseUri(new PnpConfiguration()))
        .isEqualTo("s3a://my-bucket/warehouse/staging/pnp");
  }

  /** Tests that a downloadLocation already carrying a scheme is used verbatim. */
  @Test
  void preservesSchemedDownloadLocation() {
    final ImportPnpExecutor executor =
        new ImportPnpExecutor(
            new ServerConfiguration(),
            mock(ImportExecutor.class),
            new Configuration(),
            "file:///irrelevant");
    final PnpConfiguration pnpConfig = new PnpConfiguration();
    pnpConfig.setDownloadLocation("hdfs://nn/staging/pnp");
    assertThat(executor.resolveStagingBaseUri(pnpConfig)).isEqualTo("hdfs://nn/staging/pnp");
  }

  /**
   * Tests that a plain (schemeless) downloadLocation is interpreted as a local filesystem path.
   * This preserves backward compatibility for operators with the old default.
   */
  @Test
  void treatsSchemelessDownloadLocationAsLocal() {
    final ImportPnpExecutor executor =
        new ImportPnpExecutor(
            new ServerConfiguration(),
            mock(ImportExecutor.class),
            new Configuration(),
            "file:///irrelevant");
    final PnpConfiguration pnpConfig = new PnpConfiguration();
    pnpConfig.setDownloadLocation("/usr/share/staging/pnp");
    assertThat(executor.resolveStagingBaseUri(pnpConfig))
        .isEqualTo("file:///usr/share/staging/pnp");
  }

  // ========================================
  // Manifest URL Validation Tests
  // ========================================

  /** Tests that manifest URLs on the same origin as the export URL are accepted. */
  @Test
  void acceptsManifestUrlsOnSameOrigin() {
    final BulkExportResult result =
        BulkExportResult.of(
            Instant.now(),
            List.of(
                FileResult.of(
                    URI.create("https://trusted.example.com/data/Patient.ndjson"),
                    URI.create("file:/tmp/Patient.ndjson"),
                    100L)));

    assertThatCode(
            () ->
                ImportPnpExecutor.validateManifestUrls("https://trusted.example.com/fhir", result))
        .doesNotThrowAnyException();
  }

  /** Tests that manifest URLs on a different origin than the export URL are rejected. */
  @Test
  void rejectsManifestUrlsOnDifferentOrigin() {
    final BulkExportResult result =
        BulkExportResult.of(
            Instant.now(),
            List.of(
                FileResult.of(
                    URI.create("https://evil.com/data/Patient.ndjson"),
                    URI.create("file:/tmp/Patient.ndjson"),
                    100L)));

    assertThatThrownBy(
            () ->
                ImportPnpExecutor.validateManifestUrls("https://trusted.example.com/fhir", result))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("Manifest download URL origin does not match export URL origin");
  }

  /** Tests that origin extraction works correctly for standard HTTP and HTTPS URLs. */
  @Test
  void extractsOriginCorrectly() {
    assertThat(ImportPnpExecutor.originOf(URI.create("https://example.com/path")))
        .isEqualTo("https://example.com");
    assertThat(ImportPnpExecutor.originOf(URI.create("http://example.com/path")))
        .isEqualTo("http://example.com");
    assertThat(ImportPnpExecutor.originOf(URI.create("https://example.com:8443/path")))
        .isEqualTo("https://example.com:8443");
    assertThat(ImportPnpExecutor.originOf(URI.create("http://example.com:8080/path")))
        .isEqualTo("http://example.com:8080");
  }

  /** Tests that origin extraction normalises scheme and host to lowercase. */
  @Test
  void normalisesOriginToLowercase() {
    assertThat(ImportPnpExecutor.originOf(URI.create("HTTPS://EXAMPLE.COM/path")))
        .isEqualTo("https://example.com");
  }
}
