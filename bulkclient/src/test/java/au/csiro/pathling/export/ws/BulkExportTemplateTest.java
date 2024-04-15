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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.export.BulkExportException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BulkExportTemplateTest {
  
  final AsyncConfig asyncConfig = AsyncConfig.builder()
      .minPoolingDelay(Duration.ofSeconds(1))
      .maxPoolingDelay(Duration.ofSeconds(60))
      .transientErrorDelay(Duration.ofSeconds(2))
      .tooManyRequestsDelay(Duration.ofSeconds(5))
      .maxTransientErrors(3)
      .build();

  @Mock
  BulkExportAsyncService asyncService;

  @Mock
  AsyncResponseCallback<BulkExportResponse, String> callback;

  @Captor
  ArgumentCaptor<Duration> callbackTimeout;

  BulkExportTemplate template;

  @BeforeEach
  void setUp() {
    template = new BulkExportTemplate(asyncService, asyncConfig);
  }


  @Test
  void testSuccessfulInteraction() throws Exception {
    final BulkExportRequest request = BulkExportRequest.builder()
        ._since(Instant.now())
        .build();
    final BulkExportResponse response = BulkExportResponse.builder()
        .transactionTime(Instant.now())
        .request("http://example.com")
        .build();
    final Duration timeout = Duration.ofSeconds(10);

    final URI poolingURI = URI.create("http://example.com/pool/1");
    when(asyncService.kickOff(eq(request)))
        .thenReturn(
            AcceptedAsyncResponse.builder().contentLocation(Optional.of(poolingURI.toString()))
                .build());
    when(asyncService.checkStatus(eq(poolingURI))).thenReturn(response);
    when(callback.handleResponse(eq(response), callbackTimeout.capture())).thenReturn("output");
    final String result = template.export(request, callback, timeout);

    // the callback timeout is less than the initial timeout
    assertTrue(callbackTimeout.getValue().compareTo(timeout) < 0);

    assertEquals("output", result);
    // check that the request was cleaned up
    verify(asyncService).cleanup(eq(poolingURI));
  }


  @Test
  void testErrorInKickoff() throws Exception {
    final BulkExportRequest request = BulkExportRequest.builder()
        ._since(Instant.now())
        .build();
    final Duration timeout = Duration.ofSeconds(10);

    final URI poolingURI = URI.create("http://example.com/pool/1");
    when(asyncService.kickOff(eq(request)))
        .thenThrow(new IOException("error"));
    final BulkExportException ex = assertThrows(BulkExportException.class,
        () -> template.export(request, callback, timeout));
    assertEquals("System error in bulk export", ex.getMessage());
    // no cleanup should be called
    verify(asyncService, Mockito.never()).cleanup(eq(poolingURI));
    verifyNoInteractions(callback);
  }

  @Test
  void testErrorInProgress() throws Exception {
    final BulkExportRequest request = BulkExportRequest.builder()
        ._since(Instant.now())
        .build();

    final Duration timeout = Duration.ofSeconds(10);
    final URI poolingURI = URI.create("http://example.com/pool/1");
    when(asyncService.kickOff(eq(request)))
        .thenReturn(
            AcceptedAsyncResponse.builder().contentLocation(Optional.of(poolingURI.toString()))
                .build());

    when(asyncService.checkStatus(eq(poolingURI))).thenThrow(new IOException("error"));
    final BulkExportException ex = assertThrows(BulkExportException.class,
        () -> template.export(request, callback, timeout));
    assertEquals("System error in bulk export", ex.getMessage());
    // cleanup should be called
    verify(asyncService).cleanup(eq(poolingURI));
    verifyNoInteractions(callback);
  }


  @Test
  void testErrorInCallback() throws Exception {
    final BulkExportRequest request = BulkExportRequest.builder()
        ._since(Instant.now())
        .build();
    final BulkExportResponse response = BulkExportResponse.builder()
        .transactionTime(Instant.now())
        .request("http://example.com")
        .build();
    final Duration timeout = Duration.ofSeconds(10);

    final URI poolingURI = URI.create("http://example.com/pool/1");
    when(asyncService.kickOff(eq(request)))
        .thenReturn(
            AcceptedAsyncResponse.builder().contentLocation(Optional.of(poolingURI.toString()))
                .build());
    when(asyncService.checkStatus(eq(poolingURI))).thenReturn(response);
    when(callback.handleResponse(eq(response), callbackTimeout.capture())).thenThrow(
        new IOException("error"));

    final BulkExportException ex = assertThrows(BulkExportException.class,
        () -> template.export(request, callback, timeout));
    assertEquals("System error in bulk export", ex.getMessage());
    // cleanup should be called
    verify(asyncService).cleanup(eq(poolingURI));
  }

  @Test
  void testIgnoresErrorsInCleanup() throws Exception {
    final BulkExportRequest request = BulkExportRequest.builder()
        ._since(Instant.now())
        .build();
    final BulkExportResponse response = BulkExportResponse.builder()
        .transactionTime(Instant.now())
        .request("http://example.com")
        .build();
    final Duration timeout = Duration.ofSeconds(10);

    final URI poolingURI = URI.create("http://example.com/pool/2");
    when(asyncService.kickOff(eq(request)))
        .thenReturn(
            AcceptedAsyncResponse.builder().contentLocation(Optional.of(poolingURI.toString()))
                .build());
    when(asyncService.checkStatus(eq(poolingURI))).thenReturn(response);
    doThrow(new IOException("error")).when(asyncService).cleanup(eq(poolingURI));
    when(callback.handleResponse(eq(response), callbackTimeout.capture())).thenReturn("output/2");
    final String result = template.export(request, callback, timeout);
    assertEquals("output/2", result);
    verify(asyncService).cleanup(eq(poolingURI));
  }
}
