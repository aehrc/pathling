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

package au.csiro.pathling.export.ws;

import au.csiro.pathling.export.BulkExportException;
import au.csiro.pathling.export.BulkExportException.HttpError;
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
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;


@Slf4j
public class BulkExportTemplate {


  private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

  private static final DateTimeFormatter FHIR_INSTANT_FORMAT = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX").withZone(UTC_ZONE_ID);


  private static final int HTTP_TOO_MANY_REQUESTS = 429;

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

  final Duration transientErrorDelay = Duration.ofSeconds(2);
  final Duration tooManyRequestsDelay = Duration.ofSeconds(2);

  final private int maxTransientErrors = 3;

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

    final AsyncResponse asyncResponse = httpClient.execute(httpRequest,
        AsynResponseHandler.of(BulkExportResponse.class));

    if (asyncResponse instanceof AcceptedAsyncResponse) {
      return Optional.of(asyncResponse)
          .map(AcceptedAsyncResponse.class::cast)
          .flatMap(AcceptedAsyncResponse::getContentLocation)
          .map(URI::create).orElseThrow();
    } else {
      throw new BulkExportException.ProtocolError("KickOff: Unexpected response");
    }
  }

  @Nonnull
  AsyncResponse checkStatus(@Nonnull final URI statusUri)
      throws IOException {
    final HttpUriRequest statusRequest = new HttpGet(statusUri);
    statusRequest.setHeader("accept", "application/json");
    return httpClient.execute(statusRequest, AsynResponseHandler.of(BulkExportResponse.class));
  }

  @Nonnull
  BulkExportResponse pool(@Nonnull final URI poolingURI) throws IOException, InterruptedException {

    final long poolingExitTime = System.currentTimeMillis() + poolingTimeout.toMillis();

    int transientErrorCount = 0;
    Instant nextStatusCheck = Instant.now();
    while (System.currentTimeMillis() <= poolingExitTime) {
      TimeUnit.MILLISECONDS.sleep(minPoolingTimeout.toMillis());
      if ((Instant.now().isAfter(nextStatusCheck))) {
        try {
          log.debug("Pooling: " + poolingURI);
          final AsyncResponse asyncResponse = checkStatus(poolingURI);
          // reset transient error counter
          transientErrorCount = 0;
          if (asyncResponse instanceof BulkExportResponse) {
            return (BulkExportResponse) asyncResponse;
          } else {
            final AcceptedAsyncResponse acceptedResponse = (AcceptedAsyncResponse) asyncResponse;
            log.debug("Pooling: progress: '{}', retry-after: {}",
                acceptedResponse.getProgress().orElse("na"),
                acceptedResponse.getRetryAfter().map(RetryValue::toString).orElse("na"));
            final Duration timeToSleep = computeTimeToSleep(acceptedResponse.getRetryAfter(),
                minPoolingTimeout);
            log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
            nextStatusCheck = Instant.now().plus(timeToSleep);
          }
        } catch (final HttpError ex) {
          if (ex.isTransient() && ++transientErrorCount <= maxTransientErrors) {
            // log retrying a transient error
            // TODO: extract from headers
            log.debug("Pooling: Retrying transient error {} ouf of {} : '{}'",
                transientErrorCount, maxTransientErrors, ex.getMessage());
            final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
                transientErrorDelay);
            log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
            nextStatusCheck = Instant.now().plus(timeToSleep);
          } else if (HTTP_TOO_MANY_REQUESTS == ex.getStatusCode()) {
            // handle too many requests
            log.debug("Pooling: Got too many requests error with retry-after: '{}'",
                ex.getRetryAfter().map(RetryValue::toString).orElse("na"));
            final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
                tooManyRequestsDelay);
            log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
            nextStatusCheck = Instant.now().plus(timeToSleep);
          } else {
            // TODO: Add handling of back-off error
            log.debug("Pooling: Http error: {}", ex.getMessage());
            throw ex;
          }
        }
      }
    }
    throw new BulkExportException.Timeout("Pooling timeout exceeded: " + poolingTimeout);
  }

  @Nonnull
  Duration computeTimeToSleep(@Nonnull final Optional<RetryValue> requestedDuration,
      @Nonnull final Duration defaultDuration) {
    Duration result = requestedDuration
        .map(rv -> rv.until(Instant.now())).orElse(defaultDuration);

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
}


