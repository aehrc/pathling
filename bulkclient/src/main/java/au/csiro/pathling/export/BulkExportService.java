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

import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;


@Slf4j
class BulkExportService {

  @Nonnull
  final HttpClient httpClient;

  @Nonnull
  final URI endpointUri;

  /**
   * Pooling timeout in seconds
   */
  final Duration minPoolingTimeout = Duration.ofSeconds(1);

  final Duration maxPoolingTimeout = Duration.ofSeconds(10);

  final Duration poolingTimeout = Duration.ofSeconds(60);


  final transient Gson gson = new Gson();

  public BulkExportService(@Nonnull final HttpClient httpClient, @Nonnull final URI endpoingUri) {
    this.httpClient = httpClient;
    this.endpointUri = endpoingUri;
  }

  @Nonnull
  public BulkExportResponse export(@Nonnull final BulkExportRequest request)
      throws URISyntaxException, IOException, InterruptedException {
    return pool(kickOff(request));
  }

  @Nonnull
  URI kickOff(@Nonnull final BulkExportRequest request)
      throws IOException, InterruptedException, URISyntaxException {

    final URI requestUri = toRequestURI(endpointUri, request);
    final HttpRequest httpRequest = HttpRequest.newBuilder()
        .uri(requestUri)
        .header("accept", "application/fhir+json")
        .header("prefer", "respond-async")
        .build();

    log.debug("KickOff: Request: {}", requestUri);
    final HttpResponse<String> httpResponse = httpClient.send(httpRequest,
        HttpResponse.BodyHandlers.ofString());

    if (httpResponse.statusCode() == HttpStatus.SC_ACCEPTED) {
      return httpResponse.headers().firstValue("content-location").map(URI::create).orElseThrow(
          () -> new IllegalStateException(
              "KickOff: No content-location header found in kick-off response"));
    } else {
      throw new IOException("KickOff: HTTP Error in response: " + httpResponse.statusCode());
    }
  }

  @Nonnull
  Either<BulkExportResponse, Optional<Integer>> checkStatus(@Nonnull final URI statusUri)
      throws IOException, InterruptedException {
    final HttpRequest statusRequest = HttpRequest.newBuilder()
        .uri(statusUri)
        .header("accept", "application/json")
        .build();

    final HttpResponse<String> statusResponse = httpClient.send(statusRequest,
        HttpResponse.BodyHandlers.ofString());

    if (statusResponse.statusCode() == HttpStatus.SC_OK) {
      return Either.right(gson.fromJson(statusResponse.body(), BulkExportResponse.class));
    } else if (statusResponse.statusCode() == HttpStatus.SC_ACCEPTED) {
      return Either.left(
          statusResponse.headers().firstValue("retry-after")
              .flatMap(BulkExportService::parseRetryAfter));
    } else {
      throw new IOException("CheckStatus: HTTP Error in response: " + statusResponse.statusCode());
    }
  }

  @Nonnull
  BulkExportResponse pool(@Nonnull final URI poolingURI) throws IOException, InterruptedException {

    final long poolingExitTime = System.currentTimeMillis() + poolingTimeout.toMillis();
    while (System.currentTimeMillis() <= poolingExitTime) {
      TimeUnit.MILLISECONDS.sleep(minPoolingTimeout.toMillis());
      log.debug("Pooling: " + poolingURI);
      final Either<BulkExportResponse, Optional<Integer>> statusResponse = checkStatus(poolingURI);
      if (!statusResponse.isEmpty()) {
        return statusResponse.getRight();
      } else {
        final Duration timeToSleep = computeTimeToSleep(
            statusResponse.getLeft().map(Duration::ofSeconds),
            Duration.ofMillis(poolingExitTime - System.currentTimeMillis())
        );
        log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
        TimeUnit.MILLISECONDS.sleep(timeToSleep.toMillis());
      }
    }
    throw new IOException("Pooling timeout exceeded: " + poolingTimeout);
  }

  @Nonnull
  Duration computeTimeToSleep(@Nonnull final Optional<Duration> requestedDuration,
      @Nonnull final Duration maxDuration) {
    Duration result = requestedDuration.orElse(minPoolingTimeout);
    if (result.compareTo(maxDuration) > 0) {
      result = maxDuration;
    }
    if (result.compareTo(maxPoolingTimeout) > 0) {
      result = maxPoolingTimeout;
    }
    if (result.compareTo(minPoolingTimeout) < 0) {
      result = minPoolingTimeout;
    }
    return result;
  }


  @Nonnull
  static Optional<Integer> parseRetryAfter(@Nonnull final String retryAfter) {
    try {
      return Optional.of(Integer.parseInt(retryAfter));
    } catch (final NumberFormatException __) {
      return Optional.empty();
    }
  }
  
  @Nonnull
  static URI toRequestURI(@Nonnull final URI endpointUri, @Nonnull final BulkExportRequest request)
      throws URISyntaxException {
    return new URIBuilder(endpointUri)
        .addParameter("_outputFormat", request.getOutputFormat())
        .addParameter("_type", String.join(",", request.getType()))
        .build();
  }
}


