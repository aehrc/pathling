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

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.export.BulkExportResult.FileResult;
import au.csiro.pathling.export.download.UrlDownloadTemplate;
import au.csiro.pathling.export.download.UrlDownloadTemplate.UrlDownloadEntry;
import au.csiro.pathling.export.fhir.Reference;
import au.csiro.pathling.export.fs.FileStore;
import au.csiro.pathling.export.fs.FileStore.FileHandle;
import au.csiro.pathling.export.fs.FileStoreFactory;
import au.csiro.pathling.export.utils.ExecutorServiceResource;
import au.csiro.pathling.export.utils.HttpClientConfiguration;
import au.csiro.pathling.export.ws.AsyncConfig;
import au.csiro.pathling.export.ws.BulkExportRequest;
import au.csiro.pathling.export.ws.BulkExportRequest.GroupLevel;
import au.csiro.pathling.export.ws.BulkExportRequest.PatientLevel;
import au.csiro.pathling.export.ws.BulkExportRequest.SystemLevel;
import au.csiro.pathling.export.ws.BulkExportResponse;
import au.csiro.pathling.export.ws.BulkExportTemplate;
import com.google.common.collect.Streams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;


/**
 * A client for the FHIR Bulk Data Export API.
 *
 * @see <a href="https://build.fhir.org/ig/HL7/bulk-data/export.html">FHIR Bulk Export</a>
 */
@Value
@Slf4j
@Builder(setterPrefix = "with")
public class BulkExportClient {

  @Nonnull
  String fhirEndpointUrl;

  @Nonnull
  @Builder.Default
  BulkExportRequest.Operation operation = new SystemLevel();

  @Nonnull
  @Builder.Default
  String outputFormat = "application/fhir+ndjson";

  @Nullable
  @Builder.Default
  Instant since = null;

  @Nonnull
  @Singular("type")
  List<String> types;

  @Nonnull
  @Singular("patient")
  List<Reference> patients;

  @Nonnull
  String outputDir;

  @Nonnull
  @Builder.Default
  String outputExtension = "ndjson";

  @Nonnull
  @Builder.Default
  Duration timeOut = Duration.ZERO;

  @Builder.Default
  @Min(1)
  int maxConcurrentDownloads = 10;

  @Nonnull
  @Builder.Default
  FileStoreFactory fileStoreFactory = FileStoreFactory.getLocal();

  @Nonnull
  @Builder.Default
  HttpClientConfiguration httpClientConfig = HttpClientConfiguration.builder().build();

  @Nonnull
  @Builder.Default
  AsyncConfig asyncConfig = AsyncConfig.builder().build();


  @Nonnull
  public static BulkExportClientBuilder systemBuilder() {
    return BulkExportClient.builder().withOperation(new SystemLevel());
  }

  @Nonnull
  public static BulkExportClientBuilder patientBuilder() {
    return BulkExportClient.builder().withOperation(new PatientLevel());
  }

  @Nonnull
  public static BulkExportClientBuilder groupBuilder(@Nonnull final String groupId) {
    return BulkExportClient.builder().withOperation(new GroupLevel(groupId));
  }

  public BulkExportResult export()
      throws IOException, InterruptedException, URISyntaxException {

    try (
        final FileStore fileStore = createFileStore();
        final CloseableHttpClient httpClient = createHttpClient();
        final ExecutorServiceResource executorServiceResource = createExecutorServiceResource()
    ) {
      final BulkExportTemplate bulkExportTemplate = new BulkExportTemplate(httpClient,
          URI.create(fhirEndpointUrl),
          asyncConfig);
      final UrlDownloadTemplate downloadTemplate = new UrlDownloadTemplate(httpClient,
          executorServiceResource.getExecutorService());

      final BulkExportResult result = doExport(fileStore, bulkExportTemplate, downloadTemplate);
      log.info("Export successful: {}", result);
      return result;
    }
  }

  BulkExportResult doExport(@Nonnull final FileStore fileStore,
      @Nonnull final BulkExportTemplate bulkExportTemplate,
      @Nonnull final UrlDownloadTemplate downloadTemplate)
      throws URISyntaxException, IOException, InterruptedException {

    final Instant timeOutAt = timeOut != Duration.ZERO
                              ? Instant.now().plus(timeOut)
                              : Instant.MAX;

    log.debug("Setting time out at: {} for requested timeout of: {}", timeOutAt, timeOut);

    final FileHandle destinationDir = fileStore.get(outputDir);

    if (destinationDir.exists()) {
      throw new BulkExportException(
          "Destination directory already exists: " + destinationDir.getLocation());
    } else {
      log.debug("Creating destination directory: {}", destinationDir.getLocation());
      destinationDir.mkdirs();
    }
    final BulkExportResponse response = bulkExportTemplate.export(
        buildBulkExportRequest()
    );
    log.debug("Export request completed: {}", response);
    final List<UrlDownloadEntry> downloadList = getUrlDownloadEntries(response, destinationDir);
    log.debug("Downloading entries: {}", downloadList);
    final List<Long> fileSizes = downloadTemplate.download(downloadList, timeOutAt);
    final FileHandle successMarker = destinationDir.child("_SUCCESS");
    log.debug("Marking download as complete with: {}", successMarker.getLocation());
    successMarker.writeAll(new ByteArrayInputStream(new byte[0]));
    return buildResult(response, downloadList, fileSizes);
  }

  private BulkExportRequest buildBulkExportRequest() {
    return BulkExportRequest.builder()
        .operation(operation)
        ._outputFormat(outputFormat)
        ._type(types)
        ._since(since)
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
        Collectors.groupingBy(BulkExportResponse.ResourceElement::getType, LinkedHashMap::new,
            mapping(BulkExportResponse.ResourceElement::getUrl, toList())));

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
    return String.format("%s_%04d.%s", resource, chunkNo, extension);
  }

  private FileStore createFileStore() throws IOException {
    log.debug("Creating FileStore of: {} for outputDir: {}", fileStoreFactory, outputDir);
    return fileStoreFactory.createFileStore(outputDir);
  }

  private CloseableHttpClient createHttpClient() {
    log.debug("Creating HttpClient with configuration: {}", httpClientConfig);
    return buildHttpClient(httpClientConfig);
  }

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

  private static CloseableHttpClient buildHttpClient(
      @Nonnull final HttpClientConfiguration clientConfig) {

    final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(clientConfig.getMaxConnectionsTotal());
    connectionManager.setDefaultMaxPerRoute(clientConfig.getMaxConnectionsPerRoute());

    final RequestConfig defaultRequestConfig = RequestConfig.custom()
        .setSocketTimeout(clientConfig.getSocketTimeout())
        .build();

    final HttpClientBuilder clientBuilder = HttpClients.custom()
        .setDefaultRequestConfig(defaultRequestConfig)
        .setConnectionManager(connectionManager)
        .setConnectionManagerShared(false);

    if (clientConfig.isRetryEnabled()) {
      clientBuilder.setRetryHandler(
          new DefaultHttpRequestRetryHandler(clientConfig.getRetryCount(), false));
    }
    return clientBuilder.build();
  }
}

