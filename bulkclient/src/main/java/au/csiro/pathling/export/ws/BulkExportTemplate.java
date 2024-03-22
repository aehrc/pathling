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

import static au.csiro.pathling.export.utils.TimeoutUtils.hasExpired;
import static au.csiro.pathling.export.utils.TimeoutUtils.toTimeoutAt;

import au.csiro.pathling.export.BulkExportException;
import au.csiro.pathling.export.BulkExportException.HttpError;
import au.csiro.pathling.export.fhir.FhirUtils;
import au.csiro.pathling.export.fhir.JsonSupport;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.ToString;
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
public class BulkExportTemplate {

  private static final int HTTP_TOO_MANY_REQUESTS = 429;

  private static final ContentType APPLICATION_FHIR_JSON = ContentType.create(
      "application/fhir+json",
      Consts.UTF_8);

  @Nonnull
  final HttpClient httpClient;

  @Nonnull
  final URI fhirEndpointUri;

  @Nonnull
  final AsyncConfig config;

  public BulkExportTemplate(@Nonnull final HttpClient httpClient, @Nonnull final URI endpoingUri,
      @Nonnull final AsyncConfig config) {
    this.httpClient = httpClient;
    this.fhirEndpointUri = endpoingUri;
    this.config = config;
  }
  
  @Nonnull
  public BulkExportResponse export(@Nonnull final BulkExportRequest request,
      @Nonnull final Duration timeout) {
    try {
      return pool(kickOff(request), timeout);
    } catch (final IOException | URISyntaxException | InterruptedException ex) {
      throw new BulkExportException.SystemError("System error in bulk export", ex);
    }
  }

  @Nonnull
  public BulkExportResponse export(@Nonnull final BulkExportRequest request)
      throws URISyntaxException, IOException, InterruptedException {
    return export(request, Duration.ZERO);
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
  BulkExportResponse pool(@Nonnull final URI poolingURI, @Nonnull final Duration timeout)
      throws IOException, InterruptedException {

    final Instant timeoutAt = toTimeoutAt(timeout);
    int transientErrorCount = 0;
    Instant nextStatusCheck = Instant.now();
    while (!hasExpired(timeoutAt)) {
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
          if (ex.isTransient() && ++transientErrorCount <= config.getMaxTransientErrors()) {
            // log retrying a transient error
            log.debug("Pooling: Retrying transient error {} ouf of {} : '{}'",
                transientErrorCount, config.getMaxTransientErrors(), ex.getMessage());
            final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
                config.getTransientErrorDelay());
            log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
            nextStatusCheck = Instant.now().plus(timeToSleep);
          } else if (HTTP_TOO_MANY_REQUESTS == ex.getStatusCode()) {
            // handle too many requests
            log.debug("Pooling: Got too many requests error with retry-after: '{}'",
                ex.getRetryAfter().map(RetryValue::toString).orElse("na"));
            final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
                config.getTooManyRequestsDelay());
            log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
            nextStatusCheck = Instant.now().plus(timeToSleep);
          } else {
            log.debug("Pooling: Http error: {}", ex.getMessage());
            throw ex;
          }
        }
      }
    }
    log.error("Cancelling pooling due to time limit {} exceeded at: {}", timeout, timeoutAt);
    throw new BulkExportException.Timeout(
        "Pooling timeout exceeded: " + timeout + " at: " + timeoutAt);
  }

  @Nonnull
  Duration computeTimeToSleep(@Nonnull final Optional<RetryValue> requestedDuration,
      @Nonnull final Duration defaultDuration) {
    Duration result = requestedDuration
        .map(rv -> rv.until(Instant.now())).orElse(defaultDuration);

    if (result.compareTo(config.getMaxPoolingDelay()) > 0) {
      result = config.getMaxPoolingDelay();
    }
    if (result.compareTo(config.getMinPoolingDelay()) < 0) {
      result = config.getMinPoolingDelay();
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
    if (!request.get_elements().isEmpty()) {
      uriBuilder.addParameter("_elements", String.join(",", request.get_elements()));
    }
    if (!request.get_typeFilter().isEmpty()) {
      uriBuilder.addParameter("_typeFilter", String.join(",", request.get_typeFilter()));
    }
    if (request.get_since() != null) {
      uriBuilder.addParameter("_since",
          FhirUtils.formatFhirInstant(Objects.requireNonNull(request.get_since())));
    }
    return uriBuilder.build();
  }
}


