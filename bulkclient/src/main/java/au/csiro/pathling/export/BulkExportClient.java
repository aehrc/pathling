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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
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
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;


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


  public void export()
      throws IOException, InterruptedException, URISyntaxException {

    final HttpClient httpClient = HttpClient.newHttpClient();
    final FileStore fileStore = HdfsFileStore.of(outputDir);
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    final URI endpointUrl = URI.create(fhirEndpointUrl.endsWith("/")
                                       ? fhirEndpointUrl
                                       : fhirEndpointUrl + "/").resolve("$export");

    final BulkExportService bulkExportService = new BulkExportService(httpClient, endpointUrl);
    final UrlDownloadService downloadService = new UrlDownloadService(httpClient, fileStore,
        executorService);

    final BulkExportResponse response = bulkExportService.export(
        BulkExportRequest.builder()
            ._outputFormat(outputFormat)
            ._type(type)
            ._since(since)
            .build()
    );
    log.debug("Export request completed: {}", response);

    final List<UrlDownloadEntry> downloadList = getUrlDownloadEntries(response);
    log.debug("Downloading entries: {}", downloadList);

    downloadService.download(downloadList);
    log.debug("Download completed: cleaning up resources");
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.SECONDS);
    executorService.shutdownNow();
  }

  @Nonnull
  List<UrlDownloadEntry> getUrlDownloadEntries(@Nonnull final BulkExportResponse response) {
    final Map<String, List<String>> urlsByType = response.getOutput().stream().collect(
        Collectors.groupingBy(BulkExportResponse.ResourceElement::getType, LinkedHashMap::new,
            mapping(BulkExportResponse.ResourceElement::getUrl, toList())));

    return urlsByType.entrySet().stream()
        .flatMap(entry -> IntStream.range(0, entry.getValue().size())
            .mapToObj(index -> new UrlDownloadEntry(
                    URI.create(entry.getValue().get(index)),
                    toFileName(entry.getKey(), index, outputExtension)
                )
            )
        ).collect(Collectors.toUnmodifiableList());
  }

  @Nonnull
  static String toFileName(@Nonnull final String resource, final int chunkNo,
      @Nonnull final String extension) {
    return String.format("%s_%04d.%s", resource, chunkNo, extension);
  }

  public static void main(@Nonnull final String[] args) throws Exception {

    // With transient errors
    // final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiJ0cmFuc2llbnRfZXJyb3IiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";

    final Instant from = Instant.parse("2020-01-01T00:00:00.000Z");
    // Bulk Export Demo Server
    final String fhirEndpointUrl = "https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwMCwiZHVyIjoxMCwidGx0IjoxNSwibSI6MSwic3R1Ijo0LCJkZWwiOjB9/fhir";
    final String outputDir = "target/export-" + Instant.now().toEpochMilli();

    System.out.println(
        "Exporting" + "\n from: " + fhirEndpointUrl + "\n to: " + outputDir + "\n since: " + from);

    BulkExportClient.builder()
        .withFhirEndpointUrl(fhirEndpointUrl)
        .withOutputDir(outputDir)
        .withType(List.of("Patient", "Condition"))
        .withSince(from)
        .build()
        .export();
  }
}

