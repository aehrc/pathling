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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
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

@Value
@Slf4j
public class UrlDownloadService {

  @Value
  public static class UrlDownloadEntry {

    @Nonnull
    URI uri;

    @Nonnull
    String objectName;
  }

  @Nonnull
  HttpClient httpClient;

  @Nonnull
  FileStore fileStore;

  @Nonnull
  ExecutorService executorService;

  @Value
  class UriDownloadTask implements Callable<URI> {

    @Nonnull
    URI source;

    @Nonnull
    String fileName;

    @Override
    public URI call() throws Exception {
      final HttpResponse result = httpClient.execute(new HttpGet(source));

      if (result.getStatusLine().getStatusCode() != 200) {
        throw new IOException(
            "Failed to download: " + source + " status: " + result.getStatusLine().getStatusCode());
      }
      try (final InputStream is = result.getEntity().getContent()) {
        log.debug("Downloading:  {}  to: {}", source, fileName);
        return fileStore.writeTo(fileName, is);
      }
    }
  }

  public List<URI> download(@Nonnull final List<UrlDownloadEntry> urlsToDowload)
      throws InterruptedException, IOException {
    final Collection<Callable<URI>> tasks = urlsToDowload.stream()
        .map(e -> new UriDownloadTask(e.getUri(), e.getObjectName()))
        .collect(
            Collectors.toUnmodifiableList());

    final List<Future<URI>> futures = executorService.invokeAll(tasks);
    while (!futures.stream().allMatch(Future::isDone)) {
      TimeUnit.SECONDS.sleep(1);
    }
    final List<URI> result = futures.stream().map(f -> {
      try {
        return f.get();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    fileStore.commit();
    return result;
  }

}
