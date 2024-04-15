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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import au.csiro.pathling.export.BulkExportException;
import au.csiro.pathling.export.BulkExportException.HttpError;
import au.csiro.pathling.export.fhir.OperationOutcome;
import au.csiro.pathling.export.utils.WebUtils;
import au.csiro.pathling.export.ws.BulkExportTemplate.CompletedState;
import au.csiro.pathling.export.ws.BulkExportTemplate.KickOffState;
import au.csiro.pathling.export.ws.BulkExportTemplate.PoolingState;
import au.csiro.pathling.export.ws.BulkExportTemplate.State;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BulkExportTemplateStatesTest {

  final static OperationOutcome TRANSIENT_ERROR = OperationOutcome.builder()
      .issue(List.of(
          OperationOutcome.Issue.builder()
              .severity("error")
              .code("transient")
              .diagnostics("Transient error")
              .build()
      )).build();


  final AsyncConfig asyncConfig = AsyncConfig.builder()
      .minPoolingDelay(Duration.ofSeconds(1))
      .maxPoolingDelay(Duration.ofSeconds(60))
      .transientErrorDelay(Duration.ofSeconds(2))
      .tooManyRequestsDelay(Duration.ofSeconds(5))
      .maxTransientErrors(3)
      .build();
  @Mock
  BulkExportAsyncService asyncService;

  BulkExportTemplate template;


  @BeforeEach
  void setUp() {
    template = new BulkExportTemplate(asyncService, asyncConfig);
  }

  @Test
  void testKickOffTransitionsToPoolingOnAcceptedResponse() throws IOException {

    final BulkExportRequest request = BulkExportRequest.builder()._since(Instant.now()).build();

    when(asyncService.kickOff(eq(request))).thenReturn(
        AcceptedAsyncResponse
            .builder().contentLocation(Optional.of("http://foo.bar/fhir/$pool/1"))
            .build());

    final KickOffState kickOffState = template.new KickOffState(request);
    final State nextState = kickOffState.handle(asyncService);
    assertEquals(
        template.new PoolingState(URI.create("http://foo.bar/fhir/$pool/1"), 0, Duration.ZERO),
        nextState);
  }

  @Test
  void testPoolingTransitionsToPoolingOnAcceptedResponseAndResetsErrorCount() throws IOException {

    final URI statusURI = URI.create("http://foo.bar/fhir/$pool/2");
    when(asyncService.checkStatus(eq(statusURI))).thenReturn(
        AcceptedAsyncResponse.builder()
            .retryAfter(Optional.of(RetryValue.after(Duration.ofSeconds(5))))
            .build());
    final PoolingState poolingState = template.new PoolingState(statusURI, 3, Duration.ZERO);
    final State nextState = poolingState.handle(asyncService);
    assertEquals(
        template.new PoolingState(statusURI, 0,
            Duration.ofSeconds(5)),
        nextState);
  }

  @Test
  void testPoolingTransitionsToFinalOnSuccess() throws IOException {
    final BulkExportResponse finalResponse = BulkExportResponse.builder()
        .transactionTime(Instant.now())
        .request("http://foo.bar/fhir/fhir$export/122323232")
        .build();

    final URI statusURI = URI.create("http://foo.bar/fhir/$pool/3");

    when(asyncService.checkStatus(statusURI)).thenReturn(finalResponse);

    final PoolingState poolingState = template.new PoolingState(statusURI, 0, Duration.ZERO);
    final State nextState = poolingState.handle(asyncService);
    assertEquals(new CompletedState(finalResponse, statusURI), nextState);
  }

  @Test
  void testPoolingTransitionsToPoolingWhenRetryingATransientError() throws IOException {
    when(asyncService.checkStatus(Mockito.any()))
        .thenThrow(
            new BulkExportException.HttpError("Internal Server Error", 500,
                Optional.of(TRANSIENT_ERROR),
                Optional.of(RetryValue.after(Duration.ofSeconds(7)))));

    final PoolingState poolingState = template.new PoolingState(
        URI.create("http://foo.bar/fhir/$pool/2"),
        0, Duration.ZERO);
    final State nextState = poolingState.handle(asyncService);
    assertEquals(
        template.new PoolingState(URI.create("http://foo.bar/fhir/$pool/2"), 1,
            Duration.ofSeconds(7)),
        nextState);
  }

  @Test
  void testPoolingFailsWhenTooManyTransientErrors() throws IOException {
    when(asyncService.checkStatus(Mockito.any()))
        .thenThrow(
            new BulkExportException.HttpError("Internal Server Error", 500,
                Optional.of(TRANSIENT_ERROR),
                Optional.of(RetryValue.after(Duration.ofSeconds(7)))));

    final PoolingState poolingState = template.new PoolingState(
        URI.create("http://foo.bar/fhir/$pool/2"),
        3, Duration.ZERO);

    final HttpError ex = assertThrows(HttpError.class, () -> poolingState.handle(asyncService));
    assertEquals(500, ex.getStatusCode());
    assertEquals(TRANSIENT_ERROR, ex.getOperationOutcome().orElseThrow());
  }


  @Test
  void testPoolingTransitionsTooPoolingOnTooManyRequests() throws IOException {
    when(asyncService.checkStatus(Mockito.any()))
        .thenThrow(
            new BulkExportException.HttpError("Too Many Requests", WebUtils.HTTP_TOO_MANY_REQUESTS,
                Optional.empty(),
                Optional.empty()));

    final PoolingState poolingState = template.new PoolingState(
        URI.create("http://foo.bar/fhir/$pool/2"),
        2, Duration.ZERO);
    final State nextState = poolingState.handle(asyncService);
    assertEquals(
        template.new PoolingState(URI.create("http://foo.bar/fhir/$pool/2"), 2,
            Duration.ofSeconds(5)), // the default tooManyRequestsDelay
        nextState);
  }
  
}
