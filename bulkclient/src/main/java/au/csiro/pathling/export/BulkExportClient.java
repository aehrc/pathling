/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.auth.AsymmetricClientCredentials;
import au.csiro.pathling.auth.ClientAuthRequestInterceptor;
import au.csiro.pathling.auth.AuthTokenProvider;
import au.csiro.pathling.auth.SymmetricClientCredentials;
import au.csiro.pathling.auth.TokenProvider;
import au.csiro.pathling.config.AuthConfiguration;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.export.BulkExportResult.FileResult;
import au.csiro.pathling.export.download.UrlDownloadTemplate;
import au.csiro.pathling.export.download.UrlDownloadTemplate.UrlDownloadEntry;
import au.csiro.pathling.export.fhir.Reference;
import au.csiro.pathling.export.fs.FileStore;
import au.csiro.pathling.export.fs.FileStore.FileHandle;
import au.csiro.pathling.export.fs.FileStoreFactory;
import au.csiro.pathling.export.utils.ExecutorServiceResource;
import au.csiro.pathling.export.utils.TimeoutUtils;
import au.csiro.pathling.export.ws.AsyncConfig;
import au.csiro.pathling.export.ws.BulkExportAsyncService;
import au.csiro.pathling.export.ws.BulkExportRequest;
import au.csiro.pathling.export.ws.BulkExportRequest.GroupLevel;
import au.csiro.pathling.export.ws.BulkExportRequest.Level;
import au.csiro.pathling.export.ws.BulkExportRequest.PatientLevel;
import au.csiro.pathling.export.ws.BulkExportRequest.SystemLevel;
import au.csiro.pathling.export.ws.BulkExportResponse;
import au.csiro.pathling.export.ws.BulkExportTemplate;
import com.google.common.collect.Streams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;


/**
 * A client for the FHIR Bulk Data Export API.
 *
 * @see <a href="https://build.fhir.org/ig/HL7/bulk-data/export.html">FHIR Bulk Export</a>
 */
@Value
@Slf4j
@Builder(setterPrefix = "with")
public class BulkExportClient {

  /**
   * The URL of the FHIR server to export from.
   */
  @Nonnull
  String fhirEndpointUrl;

  /**
   * The operation to perform.
   */
  @Nonnull
  @Builder.Default
  Level level = new SystemLevel();

  /**
   * The format of the output data. The value of the `_outputFormat` parameter in the export
   * request.
   */
  @Nonnull
  @Builder.Default
  String outputFormat = "application/fhir+ndjson";

  /**
   * The time to start the export from. If null, the export will start from the beginning of time.
   * The value of the `_since` parameter in the export request.
   */
  @Nullable
  @Builder.Default
  Instant since = null;

  /**
   * The types of resources to export. The value of the `_type` parameter in the export request.
   */
  @Nonnull
  @Singular("type")
  List<String> types;

  /**
   * The reference to the patients to include in the export. The value of the `patient` parameter in
   * the export request.
   */
  @Nonnull
  @Singular("patient")
  List<Reference> patients;

  /**
   * The elements to include in the export. The value of the `_elements` parameter in the export
   * request.
   */
  @Nonnull
  @Singular("element")
  List<String> elements;

  /**
   * The type filters to apply to the export. The value of the `_typeFilter` parameter in the
   * export
   */
  @Nonnull
  @Singular("typeFilter")
  List<String> typeFilters;

  /**
   * The directory to write the output files to. This describes the location in the format expected
   * by the {@link FileStoreFactory} configured in this client.
   */
  @Nonnull
  String outputDir;

  /**
   * The extension to use for the output files.
   */
  @Nonnull
  @Builder.Default
  String outputExtension = "ndjson";

  /**
   * The maximum time to wait for the export to complete. If zero or negative (default), the export
   * will not time out.
   */
  @Nonnull
  @Builder.Default
  Duration timeout = Duration.ZERO;

  /**
   * The maximum number of concurrent downloads to perform.
   */
  @Builder.Default
  @Min(1)
  int maxConcurrentDownloads = 10;

  /**
   * The factory to use to create the {@link FileStore} to write the output files to.
   */
  @Nonnull
  @Builder.Default
  FileStoreFactory fileStoreFactory = FileStoreFactory.getLocal();

  /**
   * The configuration for the HTTP client.
   */
  @Nonnull
  @Builder.Default
  HttpClientConfiguration httpClientConfig = HttpClientConfiguration.builder().build();

  /**
   * The configuration for the async operations.
   */
  @Nonnull
  @Builder.Default
  AsyncConfig asyncConfig = AsyncConfig.builder().build();


  /**
   * The configuration for the authentication.
   */
  @Nonnull
  @Builder.Default
  AuthConfiguration authConfig = AuthConfiguration.builder().build();

  /**
   * A builder for the {@link BulkExportClient}.
   */
  public static class BulkExportClientBuilder {
    // empty placeholder to for javadoc to recognize the builder
  }

  /**
   * Create a builder for a system-level export.
   *
   * @return the builder configured for a system-level export
   */
  @Nonnull
  public static BulkExportClientBuilder systemBuilder() {
    return BulkExportClient.builder().withLevel(new SystemLevel());
  }

  /**
   * Create a builder for a patient-level export.
   *
   * @return the builder configured for a patient-level export
   */
  @Nonnull
  public static BulkExportClientBuilder patientBuilder() {
    return BulkExportClient.builder().withLevel(new PatientLevel());
  }

  /**
   * Create a builder for a group-level export.
   *
   * @param groupId the group ID to export
   * @return the builder configured for a group-level export
   */
  @Nonnull
  public static BulkExportClientBuilder groupBuilder(@Nonnull final String groupId) {
    return BulkExportClient.builder().withLevel(new GroupLevel(groupId));
  }

  /**
   * Export data from the FHIR server.
   *
   * @return the result of the export
   * @throws BulkExportException if the export fails
   */
  public BulkExportResult export() {
    try (
        final TokenProvider tokenProvider = createTokenProvider();
        final FileStore fileStore = createFileStore();
        final CloseableHttpClient httpClient = createHttpClient(tokenProvider);
        final ExecutorServiceResource executorServiceResource = createExecutorServiceResource()
    ) {
      final BulkExportTemplate bulkExportTemplate = new BulkExportTemplate(
          new BulkExportAsyncService(httpClient, URI.create(fhirEndpointUrl)),
          asyncConfig);
      final UrlDownloadTemplate downloadTemplate = new UrlDownloadTemplate(httpClient,
          executorServiceResource.getExecutorService());

      final BulkExportResult result = doExport(fileStore, bulkExportTemplate, downloadTemplate);
      log.info("Export successful: {}", result);
      return result;
    } catch (final IOException ex) {
      throw new BulkExportException.SystemError("System error in bulk export", ex);
    }
  }

  BulkExportResult doExport(@Nonnull final FileStore fileStore,
      @Nonnull final BulkExportTemplate bulkExportTemplate,
      @Nonnull final UrlDownloadTemplate downloadTemplate)
      throws IOException {

    final Instant timeoutAt = TimeoutUtils.toTimeoutAt(timeout);
    log.debug("Setting timeout at: {} for requested timeout of: {}", timeoutAt, timeout);

    final FileHandle destinationDir = fileStore.get(outputDir);

    if (destinationDir.exists()) {
      throw new BulkExportException(
          "Destination directory already exists: " + destinationDir.getLocation());
    } else {
      log.debug("Creating destination directory: {}", destinationDir.getLocation());
      destinationDir.mkdirs();
    }
    final BulkExportResponse response = bulkExportTemplate.export(buildBulkExportRequest(),
        TimeoutUtils.toTimeoutAfter(timeoutAt));
    log.debug("Export request completed: {}", response);

    final List<UrlDownloadEntry> downloadList = getUrlDownloadEntries(response, destinationDir);
    log.debug("Downloading entries: {}", downloadList);
    final List<Long> fileSizes = downloadTemplate.download(downloadList,
        TimeoutUtils.toTimeoutAfter(timeoutAt));
    final FileHandle successMarker = destinationDir.child("_SUCCESS");
    log.debug("Marking download as complete with: {}", successMarker.getLocation());
    successMarker.writeAll(new ByteArrayInputStream(new byte[0]));
    return buildResult(response, downloadList, fileSizes);
  }

  private BulkExportRequest buildBulkExportRequest() {
    return BulkExportRequest.builder()
        .level(level)
        ._outputFormat(outputFormat)
        ._type(types)
        ._since(since)
        ._elements(elements)
        ._typeFilter(typeFilters)
        .patient(patients)
        .build();
  }

  @Nonnull
  private BulkExportResult buildResult(@Nonnull final BulkExportResponse response,
      @Nonnull final List<UrlDownloadEntry> downloadList, @Nonnull final List<Long> fileSizes) {

    return BulkExportResult.of(
        response.getTransactionTime(),
        Streams.zip(downloadList.stream(), fileSizes.stream(),
                (de, size) -> FileResult.of(de.getSource(), de.getDestination().toUri(), size))
            .collect(Collectors.toUnmodifiableList())
    );
  }

  @Nonnull
  List<UrlDownloadEntry> getUrlDownloadEntries(@Nonnull final BulkExportResponse response,
      @Nonnull final FileHandle destinationDir) {
    final Map<String, List<String>> urlsByType = response.getOutput().stream().collect(
        Collectors.groupingBy(BulkExportResponse.FileItem::getType, LinkedHashMap::new,
            mapping(BulkExportResponse.FileItem::getUrl, toList())));

    return urlsByType.entrySet().stream()
        .flatMap(entry -> IntStream.range(0, entry.getValue().size())
            .mapToObj(index -> new UrlDownloadEntry(
                    URI.create(entry.getValue().get(index)),
                    destinationDir.child(toFileName(entry.getKey(), index, outputExtension))
                )
            )
        ).collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  static String toFileName(@Nonnull final String resource, final int chunkNo,
      @Nonnull final String extension) {
    return String.format("%s.%04d.%s", resource, chunkNo, extension);
  }

  @Nonnull
  private FileStore createFileStore() throws IOException {
    log.debug("Creating FileStore of: {} for outputDir: {}", fileStoreFactory, outputDir);
    return fileStoreFactory.createFileStore(outputDir);
  }

  @Nonnull
  private TokenProvider createTokenProvider() {
    return new AuthTokenProvider(authConfig);
  }


  @Nonnull
  private CloseableHttpClient createHttpClient(@Nonnull final TokenProvider tokenProvider) {
    log.debug("Creating HttpClient with configuration: {}", httpClientConfig);

    final URI endpointURI = URI.create(fhirEndpointUrl);
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    createCredentials().ifPresent(cr -> credentialsProvider.setCredentials(
        new AuthScope(endpointURI.getHost(), endpointURI.getPort()), cr));
    return httpClientConfig.clientBuilder()
        .setDefaultCredentialsProvider(credentialsProvider)
        .addInterceptorFirst(new ClientAuthRequestInterceptor(tokenProvider))
        .build();
  }

  private Optional<? extends Credentials> createCredentials() {
    if (authConfig.isEnabled()) {
      if (nonNull(authConfig.getPrivateKeyJWK())) {
        return Optional.of(AsymmetricClientCredentials.builder()
            .tokenEndpoint(requireNonNull(authConfig.getTokenEndpoint()))
            .clientId(requireNonNull(authConfig.getClientId()))
            .privateKeyJWK(requireNonNull(authConfig.getPrivateKeyJWK()))
            .scope(authConfig.getScope())
            .build());
      } else {
        return Optional.of(SymmetricClientCredentials.builder()
            .tokenEndpoint(requireNonNull(authConfig.getTokenEndpoint()))
            .clientId(requireNonNull(authConfig.getClientId()))
            .clientSecret(requireNonNull(authConfig.getClientSecret()))
            .scope(authConfig.getScope())
            .sendClientCredentialsInBody(authConfig.isUseFormForBasicAuth())
            .build());
      }
    } else {
      return Optional.empty();
    }
  }

  @Nonnull
  private ExecutorServiceResource createExecutorServiceResource() {
    if (maxConcurrentDownloads <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentDownloads must be positive: " + maxConcurrentDownloads);
    }
    if (httpClientConfig.getMaxConnectionsPerRoute() < maxConcurrentDownloads) {
      log.warn("maxConnectionsPerRoute is less than maxConcurrentDownloads: {} < {}",
          httpClientConfig.getMaxConnectionsPerRoute(), maxConcurrentDownloads);
    }
    log.debug("Creating ExecutorService with maxConcurrentDownloads: {}", maxConcurrentDownloads);
    return ExecutorServiceResource.of(Executors.newFixedThreadPool(maxConcurrentDownloads));
  }
}

