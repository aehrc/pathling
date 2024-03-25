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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.export.BulkExportException.DownloadError;
import au.csiro.pathling.export.BulkExportException.HttpError;
import au.csiro.pathling.export.BulkExportException.Timeout;
import au.csiro.pathling.export.download.UrlDownloadTemplate.UrlDownloadEntry;
import au.csiro.pathling.export.fs.FileStore.FileHandle;
import au.csiro.pathling.export.utils.TimeoutUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UrlDownloadTemplateTest {

  @Mock
  ExecutorService executorService;

  @Mock
  HttpClient httpClient;

  @Mock
  HttpResponse httpResponse;

  @Mock
  FileHandle fileHandle;

  @Test
  void testDownloadsAllUrlsSuccessfully() {
    final UrlDownloadTemplate template = new UrlDownloadTemplate(httpClient, executorService);
    when(executorService.submit(eq(template.new UriDownloadTask(URI.create("http://foo.bar/file1"),
        FileHandle.ofLocal("file1"))))).thenReturn(CompletableFuture.completedFuture(3L));
    when(executorService.submit(eq(template.new UriDownloadTask(URI.create("http://foo.bar/file2"),
        FileHandle.ofLocal("file2"))))).thenReturn(CompletableFuture.completedFuture(7L));

    final List<Long> result = template.download(List.of(
        new UrlDownloadEntry(URI.create("http://foo.bar/file1"), FileHandle.ofLocal("file1")),
        new UrlDownloadEntry(URI.create("http://foo.bar/file2"), FileHandle.ofLocal("file2"))
    ), TimeoutUtils.TIMEOUT_INFINITE);
    assertEquals(List.of(3L, 7L), result);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testThrowsTimeoutExceptionWhenTimeout() {
    final UrlDownloadTemplate template = new UrlDownloadTemplate(httpClient, executorService);
    when(executorService.submit(Mockito.any(Callable.class))).thenReturn(new CompletableFuture<>());

    final Timeout ex = assertThrows(Timeout.class,
        () -> template.download(List.of(
            new UrlDownloadEntry(URI.create("http://foo.bar/file1"), FileHandle.ofLocal("file1")),
            new UrlDownloadEntry(URI.create("http://foo.bar/file2"), FileHandle.ofLocal("file2"))
        ), Duration.ofMillis(100)));
    assertEquals("Download timed out at: PT0.1S", ex.getMessage());
  }


  @Test
  void testFailsOnErrorFast() {
    final IOException downloadEx = new IOException("IO Error");
    final CompletableFuture<Long> runningFuture = new CompletableFuture<>();

    final UrlDownloadTemplate template = new UrlDownloadTemplate(httpClient, executorService);
    when(executorService.submit(eq(template.new UriDownloadTask(URI.create("http://foo.bar/file1"),
        FileHandle.ofLocal("file1"))))).thenReturn(
        CompletableFuture.failedFuture(downloadEx));
    when(executorService.submit(eq(template.new UriDownloadTask(URI.create("http://foo.bar/file2"),
        FileHandle.ofLocal("file2"))))).thenReturn(runningFuture);

    final DownloadError ex = assertThrows(DownloadError.class,
        () -> template.download(List.of(
            new UrlDownloadEntry(URI.create("http://foo.bar/file1"), FileHandle.ofLocal("file1")),
            new UrlDownloadEntry(URI.create("http://foo.bar/file2"), FileHandle.ofLocal("file2"))
        ), TimeoutUtils.TIMEOUT_INFINITE));

    assertEquals("Download failed", ex.getMessage());
    assertEquals(downloadEx, ex.getCause());
    assertTrue(runningFuture.isCancelled());
  }


  @Captor
  ArgumentCaptor<HttpUriRequest> httpRequestCaptor;

  @Test
  void testDownloadTaskGetsTheFileOnSuccess() throws Exception {
    final InputStream responseStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
    when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(
        new BasicStatusLine(new ProtocolVersion("http", 1, 1), 200, "OK"));
    when(httpResponse.getEntity()).thenReturn(new InputStreamEntity(responseStream, 3));
    when(fileHandle.writeAll(Mockito.eq(responseStream))).thenReturn(7L);
    final UrlDownloadTemplate template = new UrlDownloadTemplate(httpClient, executorService);
    final UrlDownloadTemplate.UriDownloadTask task = template.new UriDownloadTask(
        URI.create("http://foo.bar/file1"),
        fileHandle);
    assertEquals(7L, task.call());
    verify(httpClient).execute(httpRequestCaptor.capture());
    assertEquals("http://foo.bar/file1", httpRequestCaptor.getValue().getURI().toString());
    assertEquals("GET", httpRequestCaptor.getValue().getMethod());
  }

  @Test
  void testDownloadTaskFailsOnHttpError() throws Exception {
    when(httpClient.execute(Mockito.any())).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(
        new BasicStatusLine(new ProtocolVersion("http", 1, 1), 500, "Internal Server Error"));
    final UrlDownloadTemplate template = new UrlDownloadTemplate(httpClient, executorService);
    final HttpError ex = assertThrows(HttpError.class, () -> template.new UriDownloadTask(
        URI.create("http://foo.bar/file1"),
        fileHandle).call());
    assertEquals("Failed to download: http://foo.bar/file1: [statusCode: 500]", ex.getMessage());
  }
}
