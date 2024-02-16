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
import au.csiro.pathling.export.utils.JsonSupport;
import au.csiro.pathling.export.fhir.FhirUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;


@Slf4j
@ToString
public class BulkExport {

  private static final int HTTP_TOO_MANY_REQUESTS = 429;

  private static final ContentType APPLICATION_FHIR_JSON = ContentType.create(
      "application/fhir+json",
      Consts.UTF_8);

  @Value
  @Builder
  public static class Config {

    /**
     * Pooling timeout in seconds
     */
    @Builder.Default
    Duration minPoolingDelay = Duration.ofSeconds(1);

    @Builder.Default
    Duration maxPoolingDelay = Duration.ofSeconds(60);

    @Builder.Default
    Duration poolingTimeout = Duration.ofHours(1);

    @Builder.Default
    Duration transientErrorDelay = Duration.ofSeconds(2);

    @Builder.Default
    Duration tooManyRequestsDelay = Duration.ofSeconds(2);

    @Builder.Default
    int maxTransientErrors = 3;
  }

  @Nonnull
  final HttpClient httpClient;

  @Nonnull
  final URI fhirEndpointUri;

  @Nonnull
  final Config config;


  public BulkExport(@Nonnull final HttpClient httpClient, @Nonnull final URI endpoingUri,
      @Nonnull final Config config) {
    this.httpClient = httpClient;
    this.fhirEndpointUri = endpoingUri;
    this.config = config;
  }

  public BulkExport(@Nonnull final HttpClient httpClient, @Nonnull final URI endpoingUri) {
    this(httpClient, endpoingUri, Config.builder().build());
  }
  
  @Nonnull
  public BulkExportResponse export(@Nonnull final BulkExportRequest request)
      throws URISyntaxException, IOException, InterruptedException {
    return pool(kickOff(request));
  }

  @Nonnull
  URI kickOff(@Nonnull final BulkExportRequest request)
      throws IOException, URISyntaxException {

    final HttpUriRequest httpRequest = toHttpRequest(fhirEndpointUri, request);
    log.debug("KickOff: Request: {}", httpRequest);
    if (httpRequest instanceof HttpPost) {
      log.debug("KickOff: Request body: {}",
          EntityUtils.toString(((HttpPost) httpRequest).getEntity()));
    }

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

    final long poolingExitTime = System.currentTimeMillis() + config.poolingTimeout.toMillis();

    int transientErrorCount = 0;
    Instant nextStatusCheck = Instant.now();
    while (System.currentTimeMillis() <= poolingExitTime) {
      TimeUnit.MILLISECONDS.sleep(config.getMinPoolingDelay().toMillis());
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
                config.getMinPoolingDelay());
            log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
            nextStatusCheck = Instant.now().plus(timeToSleep);
          }
        } catch (final HttpError ex) {
          if (ex.isTransient() && ++transientErrorCount <= config.maxTransientErrors) {
            // log retrying a transient error
            // TODO: extract from headers
            log.debug("Pooling: Retrying transient error {} ouf of {} : '{}'",
                transientErrorCount, config.maxTransientErrors, ex.getMessage());
            final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
                config.transientErrorDelay);
            log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
            nextStatusCheck = Instant.now().plus(timeToSleep);
          } else if (HTTP_TOO_MANY_REQUESTS == ex.getStatusCode()) {
            // handle too many requests
            log.debug("Pooling: Got too many requests error with retry-after: '{}'",
                ex.getRetryAfter().map(RetryValue::toString).orElse("na"));
            final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
                config.tooManyRequestsDelay);
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
    throw new BulkExportException.Timeout("Pooling timeout exceeded: " + config.poolingTimeout);
  }

  @Nonnull
  Duration computeTimeToSleep(@Nonnull final Optional<RetryValue> requestedDuration,
      @Nonnull final Duration defaultDuration) {
    Duration result = requestedDuration
        .map(rv -> rv.until(Instant.now())).orElse(defaultDuration);

    if (result.compareTo(config.maxPoolingDelay) > 0) {
      result = config.maxPoolingDelay;
    }
    if (result.compareTo(config.minPoolingDelay) < 0) {
      result = config.minPoolingDelay;
    }
    return result;
  }


  @Nonnull
  static HttpUriRequest toHttpRequest(@Nonnull final URI fhirEndpointUri,
      @Nonnull final BulkExportRequest request)
      throws URISyntaxException {

    // check if patient is supported for the operation
    if (!request.getOperation().isPatientSupported() && !request.getPatient().isEmpty()) {
      throw new BulkExportException.ProtocolError(
          "'patient' is not supported for operation: " + request.getOperation());
    }
    final URI endpointUri = ensurePathEndsWithSlash(fhirEndpointUri).resolve(
        request.getOperation().getPath());
    final HttpUriRequest httpRequest;

    if (request.getPatient().isEmpty()) {
      httpRequest = new HttpGet(toRequestURI(endpointUri, request));
    } else {
      final HttpPost postRequest = new HttpPost(endpointUri);
      postRequest.setEntity(toFhirJsonEntity(request.toParameters()));
      httpRequest = postRequest;
    }
    httpRequest.setHeader("accept", APPLICATION_FHIR_JSON.getMimeType());
    httpRequest.setHeader("prefer", "respond-async");
    return httpRequest;
  }

  @Nonnull
  static HttpEntity toFhirJsonEntity(@Nonnull final Object fhirResource) {
    return new StringEntity(JsonSupport.toJson(fhirResource), APPLICATION_FHIR_JSON);
  }

  @Nonnull
  static URI ensurePathEndsWithSlash(@Nonnull final URI uri) {
    return uri.getPath().endsWith("/")
           ? uri
           : URI.create(uri + "/");
  }

  @Nonnull
  static URI toRequestURI(@Nonnull final URI endpointUri,
      @Nonnull final BulkExportRequest request)
      throws URISyntaxException {
    final URIBuilder uriBuilder = new URIBuilder(endpointUri)
        .addParameter("_outputFormat", request.get_outputFormat());
    if (!request.get_type().isEmpty()) {
      uriBuilder.addParameter("_type", String.join(",", request.get_type()));
    }
    if (request.get_since() != null) {
      uriBuilder.addParameter("_since",
          FhirUtils.formatFhirInstant(Objects.requireNonNull(request.get_since())));
    }
    return uriBuilder.build();
  }
}


