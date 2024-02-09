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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;


@Slf4j
class BulkExportTemplate {


  private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

  private static final DateTimeFormatter FHIR_INSTANT_FORMAT = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX").withZone(UTC_ZONE_ID);


  private static final Gson GSON = new Gson();


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


  final Duration transientErrorBreak = Duration.ofSeconds(5);

  public BulkExportTemplate(@Nonnull final HttpClient httpClient, @Nonnull final URI endpoingUri) {
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
      throws IOException, URISyntaxException {

    final URI requestUri = toRequestURI(endpointUri, request);
    final HttpUriRequest httpRequest = new HttpGet(requestUri);
    httpRequest.setHeader("accept", "application/fhir+json");
    httpRequest.setHeader("prefer", "respond-async");
    log.debug("KickOff: Request: {}", requestUri);
    final HttpResponse httpResponse = httpClient.execute(httpRequest);
    final int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (statusCode == HttpStatus.SC_ACCEPTED) {
      return Optional.ofNullable(httpResponse.getFirstHeader("content-location"))
          .map(Header::getValue)
          .map(URI::create).orElseThrow(
              () -> new IllegalStateException(
                  "KickOff: No content-location header found in kick-off response"));
    } else {
      log.debug("KickOff: errorResponse: {}", httpResponse);
      throw BulkExportException.HttpError.of("KickOff: HTTP Error in response", httpResponse);
    }
  }

  @Nonnull
  Either<BulkExportResponse, Optional<RetryValue>> checkStatus(@Nonnull final URI statusUri)
      throws IOException {
    final HttpUriRequest statusRequest = new HttpGet(statusUri);
    statusRequest.setHeader("accept", "application/json");

    final HttpResponse statusResponse = httpClient.execute(statusRequest);
    log.debug("Status: responseReceived: {}", statusResponse);

    final int statusCode = statusResponse.getStatusLine().getStatusCode();
    if (statusCode == HttpStatus.SC_OK) {
      return Either.right(GSON.fromJson(EntityUtils.toString(statusResponse.getEntity()),
          BulkExportResponse.class));
    } else if (statusCode == HttpStatus.SC_ACCEPTED) {
      return Either.left(
          Optional.ofNullable(statusResponse.getFirstHeader("retry-after"))
              .map(Header::getValue)
              .flatMap(RetryValue::parse));
      // TODO: ad handling of too many requests
    } else {
      if (isTransientError(statusResponse)) {
        log.debug("CheckStatus: Transient error encountered:  {}", statusResponse);
        return Either.left(Optional.of(RetryValue.after((transientErrorBreak))));
      }
      throw BulkExportException.HttpError.of("CheckStatus: HTTP Error in response",
          statusResponse);
    }
  }

  @Nonnull
  BulkExportResponse pool(@Nonnull final URI poolingURI) throws IOException, InterruptedException {

    final long poolingExitTime = System.currentTimeMillis() + poolingTimeout.toMillis();
    while (System.currentTimeMillis() <= poolingExitTime) {
      TimeUnit.MILLISECONDS.sleep(minPoolingTimeout.toMillis());
      log.debug("Pooling: " + poolingURI);
      final Either<BulkExportResponse, Optional<RetryValue>> statusResponse = checkStatus(
          poolingURI);
      if (!statusResponse.isEmpty()) {
        return statusResponse.getRight();
      } else {
        final Duration timeToSleep = computeTimeToSleep(
            statusResponse.getLeft().map(rv -> rv.until(Instant.now())),
            Duration.ofMillis(poolingExitTime - System.currentTimeMillis()));
        log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
        TimeUnit.MILLISECONDS.sleep(timeToSleep.toMillis());
      }
    }
    throw new BulkExportException.Timeout("Pooling timeout exceeded: " + poolingTimeout);
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
  static String formatFhirInstant(@Nonnull final Instant instant) {
    return FHIR_INSTANT_FORMAT.format(instant);
  }

  @Nonnull
  static URI toRequestURI(@Nonnull final URI endpointUri, @Nonnull final BulkExportRequest request)
      throws URISyntaxException {
    final URIBuilder uriBuilder = new URIBuilder(endpointUri)
        .addParameter("_outputFormat", request.get_outputFormat())
        .addParameter("_type", String.join(",", request.get_type()));

    if (request.get_since() != null) {
      uriBuilder.addParameter("_since",
          formatFhirInstant(Objects.requireNonNull(request.get_since())));
    }
    return uriBuilder.build();
  }

  static boolean isTransientError(@Nonnull final HttpResponse statusResponse) {
    return asOperationOutcome(statusResponse).map(OperationOutcome::isTransient)
        .orElse(false);
  }

  @Nonnull
  static Optional<OperationOutcome> asOperationOutcome(
      @Nonnull final HttpResponse statusResponse) {
    try {
      return OperationOutcome.parse(EntityUtils.toString(statusResponse.getEntity()));
    } catch (final IOException e) {
      log.debug("Failed to parse OperationOutcome from response", e);
      return Optional.empty();
    }
  }

}


