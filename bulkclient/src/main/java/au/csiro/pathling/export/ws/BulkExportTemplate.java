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
import au.csiro.pathling.export.utils.TimeoutUtils;
import au.csiro.pathling.export.utils.WebUtils;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;


/**
 * A template for the FHIR Bulk Export operation. Orchestrates the FHIR asynchronous request pattern
 * for the FHIR Bulk Export operation.
 */
@Slf4j
@ToString
public class BulkExportTemplate {

  @Nonnull
  final BulkExportAsyncService asyncService;

  @Nonnull
  final AsyncConfig config;


  /**
   * Represents the state FHIR Async pattern interaction.
   */
  interface State {

    @Nonnull
    State handle(@Nonnull final BulkExportAsyncService service) throws IOException;

    void cleanup(@Nonnull final BulkExportAsyncService service);

    boolean isFinal();

    @Nonnull
    Duration getRetryAfter();
  }


  /**
   * Represents any state of the FHIR Async pattern interaction after a successful kick-off.
   */
  interface ActiveState extends State {

    @Nonnull
    URI getPoolingURI();

    @Override
    default void cleanup(@Nonnull final BulkExportAsyncService service) {
      try {
        service.cleanup(getPoolingURI());
      } catch (final IOException ex) {
        log.warn("Error cancelling pooling: " + getPoolingURI(), ex);
      }
    }
  }

  /**
   * Represents the initial state of the FHIR Async pattern interaction
   */
  @Value
  class KickOffState implements State {

    @Nonnull
    BulkExportRequest request;

    @Override
    @Nonnull
    public State handle(@Nonnull final BulkExportAsyncService service) throws IOException {
      final AsyncResponse asyncResponse = service.kickOff(request);
      if (asyncResponse instanceof AcceptedAsyncResponse) {
        return handleAcceptedResponse((AcceptedAsyncResponse) asyncResponse);
      } else {
        throw new BulkExportException.ProtocolError(
            "KickOff: Unexpected response: " + asyncResponse);
      }
    }

    @Override
    public void cleanup(@Nonnull final BulkExportAsyncService service) {
      // nothing to clean up
    }

    @Nonnull
    private PoolingState handleAcceptedResponse(
        @Nonnull final AcceptedAsyncResponse asyncResponse) {
      final URI statusURI = asyncResponse.getContentLocation()
          .map(URI::create).orElseThrow(
              () -> new BulkExportException.ProtocolError("KickOff: Missing content-location"));
      log.debug("KickOff: Accepted: " + statusURI);
      return new PoolingState(statusURI, 0, Duration.ZERO);
    }

    @Override
    public boolean isFinal() {
      return false;
    }

    @Nonnull
    @Override
    public Duration getRetryAfter() {
      return Duration.ZERO;
    }
  }


  /**
   * Represents the pooling state of the FHIR Async pattern interaction.
   */
  @Value
  class PoolingState implements ActiveState {

    @Nonnull
    URI poolingURI;

    int transientErrorCount;

    @Nonnull
    Duration retryAfter;

    @Nonnull
    @Override
    public State handle(@Nonnull final BulkExportAsyncService service) throws IOException {
      try {
        final AsyncResponse asyncResponse = service.checkStatus(poolingURI);
        // reset transient error counter
        if (asyncResponse instanceof BulkExportResponse) {
          return handleFinalResponse((BulkExportResponse) asyncResponse);
        } else {
          return handleContinueResponse((AcceptedAsyncResponse) asyncResponse);
        }
      } catch (final HttpError ex) {
        if (ex.isTransient()) {
          // log retrying a transient error
          return handleTransientError(ex);
        } else if (WebUtils.HTTP_TOO_MANY_REQUESTS == ex.getStatusCode()) {
          // handle too many requests
          return handleTooManyRequests(ex);
        } else {
          log.debug("Pooling: Http error: {}", ex.getMessage());
          throw ex;
        }
      }
    }

    @Nonnull
    private PoolingState handleTransientError(@Nonnull final HttpError ex) {
      if (transientErrorCount < config.getMaxTransientErrors()) {
        log.debug("Pooling: Retrying transient error {} ouf of {} : '{}'",
            transientErrorCount, config.getMaxTransientErrors(), ex.getMessage());
        final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
            config.getTransientErrorDelay());
        log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
        return new PoolingState(poolingURI, transientErrorCount + 1, timeToSleep);
      } else {
        log.debug("Pooling: giving up on retrying of transient error: {}", ex.getMessage());
        throw ex;
      }
    }

    @Nonnull
    private PoolingState handleTooManyRequests(@Nonnull final HttpError ex) {
      log.debug("Pooling: Got too many requests error with retry-after: '{}'",
          ex.getRetryAfter().map(RetryValue::toString).orElse("na"));
      final Duration timeToSleep = computeTimeToSleep(ex.getRetryAfter(),
          config.getTooManyRequestsDelay());
      log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
      return new PoolingState(poolingURI, transientErrorCount, timeToSleep);
    }

    @Nonnull
    private PoolingState handleContinueResponse(
        @Nonnull final AcceptedAsyncResponse acceptedResponse) {
      log.debug("Pooling: progress: '{}', retry-after: {}",
          acceptedResponse.getProgress().orElse("na"),
          acceptedResponse.getRetryAfter().map(RetryValue::toString).orElse("na"));
      final Duration timeToSleep = computeTimeToSleep(acceptedResponse.getRetryAfter(),
          config.getMinPoolingDelay());
      log.debug("Pooling: Sleeping for {} ms", timeToSleep.toMillis());
      return new PoolingState(poolingURI, 0, timeToSleep);
    }

    @Nonnull
    private CompletedState handleFinalResponse(@Nonnull final BulkExportResponse asyncResponse) {
      return new CompletedState(asyncResponse, poolingURI);
    }

    @Override
    public boolean isFinal() {
      return false;
    }
  }

  /**
   * Represents the state of successfull completion of  the FHIR Async pattern interaction.
   */
  @Value
  static class CompletedState implements ActiveState {

    @Nonnull
    BulkExportResponse response;

    @Nonnull
    URI poolingURI;

    @Nonnull
    @Override
    public State handle(@Nonnull final BulkExportAsyncService service) {
      throw new IllegalStateException("Final state cannot handle");
    }

    @Override
    public boolean isFinal() {
      return true;
    }

    @Nonnull
    @Override
    public Duration getRetryAfter() {
      return Duration.ZERO;
    }
  }

  /**
   * Creates a new instance of the template.
   *
   * @param asyncService the async service
   * @param config the async configuration
   */
  public BulkExportTemplate(@Nonnull final BulkExportAsyncService asyncService,
      @Nonnull final AsyncConfig config) {
    this.asyncService = asyncService;
    this.config = config;
  }

  /**
   * Executes the FHIR async pattern for the bulk export operation.
   *
   * @param request the bulk export request
   * @param timeout the timeout for the operation.
   * @return the final response for the bulk export operation.
   */
  @Nonnull
  public <T> T export(@Nonnull final BulkExportRequest request,
      @Nonnull final AsyncResponseCallback<BulkExportResponse, T> callback,
      @Nonnull final Duration timeout) {
    try {
      return run(new KickOffState(request), callback, timeout);
    } catch (final IOException | InterruptedException ex) {
      throw new BulkExportException.SystemError("System error in bulk export", ex);
    }
  }

  @Nonnull
  private <T> T run(@Nonnull final State initialState,
      @Nonnull final AsyncResponseCallback<BulkExportResponse, T> callback,
      @Nonnull final Duration timeout) throws IOException, InterruptedException {
    final Instant timeoutAt = toTimeoutAt(timeout);
    State currentState = initialState;
    Instant nextStatusCheck = Instant.now();
    try {
      while (!hasExpired(timeoutAt)) {
        if (Instant.now().isAfter(nextStatusCheck)) {
          currentState = currentState.handle(asyncService);
          if (currentState.isFinal()) {
            // process the callback
            return callback.handleResponse(
                ((CompletedState) currentState).getResponse(),
                TimeoutUtils.toTimeoutAfter(timeoutAt));
          } else {
            nextStatusCheck = Instant.now().plus(currentState.getRetryAfter());
          }
        }
        TimeUnit.MILLISECONDS.sleep(config.getMinPoolingDelay().toMillis());
      }
      log.error("Cancelling pooling due to time limit {} exceeded at: {}", timeout, timeoutAt);
      throw new BulkExportException.Timeout(
          "Pooling timeout exceeded: " + timeout + " at: " + timeoutAt);
    } finally {
      currentState.cleanup(asyncService);
    }
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
}


