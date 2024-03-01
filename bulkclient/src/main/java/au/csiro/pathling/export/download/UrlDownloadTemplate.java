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

package au.csiro.pathling.export.download;

import static au.csiro.pathling.export.utils.TimeoutUtils.hasExpired;
import static au.csiro.pathling.export.utils.TimeoutUtils.toTimeoutAt;

import au.csiro.pathling.auth.AuthContext;
import au.csiro.pathling.auth.AuthContext.TokenProvider;
import au.csiro.pathling.export.BulkExportException;
import au.csiro.pathling.export.BulkExportException.DownloadError;
import au.csiro.pathling.export.BulkExportException.HttpError;
import au.csiro.pathling.export.BulkExportException.Timeout;
import au.csiro.pathling.export.fs.FileStore.FileHandle;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

@Value
@Slf4j
public class UrlDownloadTemplate {

  @Value
  public static class UrlDownloadEntry {

    @Nonnull
    URI source;

    @Nonnull
    FileHandle destination;
  }

  @Nonnull
  HttpClient httpClient;

  @Nonnull
  ExecutorService executorService;

  @Value
  class UriDownloadTask implements Callable<Long> {

    @Nonnull
    URI source;

    @Nonnull
    FileHandle destination;

    @Nonnull
    TokenProvider tokenProvider;

    @Override
    public Long call() throws Exception {
      log.debug("Starting download from:  {}  to: {}", source, destination);
      final HttpResponse result = tokenProvider.withToken(
          () -> httpClient.execute(new HttpGet(source)));
      if (result.getStatusLine().getStatusCode() != 200) {
        log.error("Failed to download: {}. Status: {}. Body: {}", source,
            result.getStatusLine(), EntityUtils.toString(result.getEntity()));
        throw new HttpError(
            "Failed to download: " + source, result.getStatusLine().getStatusCode());
      }
      try (final InputStream is = result.getEntity().getContent()) {
        final long bytesWritten = destination.writeAll(is);
        log.debug("Downloaded {} bytes from:  {}  to: {}", bytesWritten, source, destination);
        return bytesWritten;
      }
    }
  }


  public List<Long> download(@Nonnull final List<UrlDownloadEntry> urlToDownload) {
    return download(urlToDownload, AuthContext.noAuthProvider(), Duration.ZERO);
  }

  public List<Long> download(@Nonnull final List<UrlDownloadEntry> urlToDownload,
      @Nonnull final TokenProvider tokenProvider,
      @Nonnull final Duration timeout) {

    final Instant timeoutAt = toTimeoutAt(timeout);

    final Collection<Callable<Long>> tasks = urlToDownload.stream()
        .map(e -> new UriDownloadTask(e.getSource(), e.getDestination(), tokenProvider))
        .collect(Collectors.toUnmodifiableList());

    // submitting the task independently
    final List<Future<Long>> futures = tasks.stream().map(executorService::submit)
        .collect(Collectors.toUnmodifiableList());

    try {
      // wait for all the futures to complete or any to fail
      while (!futures.stream().allMatch(Future::isDone)
          && futures.stream().noneMatch(f -> asException(f).isPresent())) {
        if (hasExpired(timeoutAt)) {
          log.error("Cancelling download due to time limit {} exceeded at: {}", timeout,
              timeoutAt);
          throw new Timeout("Download timed out at: " + timeout);
        }
        TimeUnit.SECONDS.sleep(1);
      }
      // check if any of the futures failed
      futures.stream().map(UrlDownloadTemplate::asException)
          .filter(Optional::isPresent).flatMap(Optional::stream)
          .findAny()
          .ifPresent(e -> {
            log.error("Cancelling the download because of '{}'", unwrap(e).getMessage());
            throw new DownloadError("Download failed", unwrap(e));
          });
      return futures.stream().map(UrlDownloadTemplate::asValue).collect(Collectors.toList());
    } catch (final InterruptedException ex) {
      log.debug("Download interrupted", ex);
      throw new BulkExportException.SystemError("Download interrupted", ex);
    } finally {
      // cancel all the futures
      futures.forEach(f -> f.cancel(true));
    }
  }

  static <T> Optional<Exception> asException(@Nonnull final Future<T> f) {
    try {
      if (f.isDone()) {
        f.get();
      }
      return Optional.empty();
    } catch (final Exception ex) {
      return Optional.of(ex);
    }
  }

  static <T> T asValue(@Nonnull final Future<T> f) {
    if (!f.isDone()) {
      throw new IllegalStateException("Future is not done");
    }
    try {
      return f.get();
    } catch (final Exception ex) {
      throw new IllegalStateException("Unexpected exception from successful future", ex);
    }
  }

  static Throwable unwrap(@Nonnull final Exception futureEx) {
    if (futureEx instanceof ExecutionException) {
      return futureEx.getCause();
    } else {
      return futureEx;
    }
  }
}
