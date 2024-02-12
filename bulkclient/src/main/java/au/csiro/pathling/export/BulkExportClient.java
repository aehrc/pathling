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

import au.csiro.pathling.export.UrlDownloadService.UrlDownloadEntry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.export.fs.FileStore;
import au.csiro.pathling.export.fs.FileStore.FileHandle;
import au.csiro.pathling.export.fs.FileStoreFactory;
import lombok.Builder;
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
  String outputFormat = "application/fhir+ndjson";


  @Nonnull
  @Builder.Default
  List<String> type = Collections.emptyList();

  @Nullable
  @Builder.Default
  Instant since = null;

  @Nonnull
  @Builder.Default
  String outputFileFormat = "";

  @Nonnull
  String outputDir;

  @Nonnull
  @Builder.Default
  String outputExtension = "ndjson";

  @Nullable
  BulkExportProgress progress;

  @Nonnull
  HttpClientConfiguration httpClientConfig = HttpClientConfiguration.builder().build();

  @Nonnull
  @Builder.Default
  FileStoreFactory fileStoreFactory = FileStoreFactory.getLocal();

  public void export()
      throws IOException, InterruptedException, URISyntaxException {
    
    final FileStore fileStore = fileStoreFactory.createFileStore(outputDir);
    final FileHandle destinationDir = fileStore.get(outputDir);
    if (destinationDir.exists()) {
      throw new BulkExportException(
          "Destination directory already exists: " + destinationDir.getLocation());
    } else {
      log.debug("Creating destination directory: {}", destinationDir.getLocation());
      destinationDir.mkdirs();
    }
    // TODO: close the fileStore and httpClient
    final CloseableHttpClient httpClient = buildHttpClient(httpClientConfig);
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    final URI endpointUrl = URI.create(fhirEndpointUrl.endsWith("/")
                                       ? fhirEndpointUrl
                                       : fhirEndpointUrl + "/").resolve("$export");

    final BulkExportTemplate bulkExportTemplate = new BulkExportTemplate(httpClient, endpointUrl);
    final UrlDownloadService downloadService = new UrlDownloadService(httpClient, executorService);

    if (progress != null) {
      progress.onStart();
    }

    final BulkExportResponse response = bulkExportTemplate.export(
        BulkExportRequest.builder()
            ._outputFormat(outputFormat)
            ._type(type)
            ._since(since)
            .build()
    );
    log.debug("Export request completed: {}", response);
    if (progress != null) {
      progress.onExportComplete(response);
    }

    final List<UrlDownloadEntry> downloadList = getUrlDownloadEntries(response, destinationDir);
    log.debug("Downloading entries: {}", downloadList);
    downloadService.download(downloadList);

    final FileHandle successMarker = destinationDir.child("_SUCCESS");
    log.debug("Marking download as complete with: {}", successMarker.getLocation());
    successMarker.writeAll(new ByteArrayInputStream(new byte[0]));

    log.debug("Download completed: cleaning up resources");
    if (progress != null) {
      progress.onComplete();
    }

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);
    executorService.shutdownNow();
    httpClient.close();
    fileStore.close();
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

