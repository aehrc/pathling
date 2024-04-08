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

import au.csiro.pathling.export.BulkExportException.HttpError;
import au.csiro.pathling.export.fhir.FhirJsonSupport;
import au.csiro.pathling.export.fhir.OperationOutcome;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.jupiter.api.Test;

class AsyncResponseHandlerTest {


  @Value
  static class MyFinalResponse implements AsyncResponse {

    @Nonnull
    String body;
  }

  @Test
  void testProcessFinalResponse() throws UnsupportedEncodingException {
    final AsynResponseHandler<MyFinalResponse> handler = AsynResponseHandler.of(
        MyFinalResponse.class);
    final HttpResponse httpResponse = new BasicHttpResponse(
        HttpVersion.HTTP_1_1, 200, "OK");
    httpResponse.setEntity(new StringEntity("{\"body\": \"Success\"}"));
    final AsyncResponse finalResponse = handler.handleResponse(httpResponse);
    assertEquals(new MyFinalResponse("Success"), finalResponse);
  }

  @Test
  void testProcessKickOffAcceptedResponse() {
    final AsynResponseHandler<MyFinalResponse> handler = AsynResponseHandler.of(
        MyFinalResponse.class);
    final HttpResponse httpResponse = new BasicHttpResponse(
        HttpVersion.HTTP_1_1, 202, "Accepted");
    httpResponse.addHeader("content-location", "http://example.com");
    httpResponse.addHeader("x-progress", "1%");
    httpResponse.addHeader("retry-after", "10");
    final AsyncResponse kickOffAcceptedResponse = handler.handleResponse(httpResponse);
    assertEquals(AcceptedAsyncResponse.builder()
            .contentLocation(Optional.of("http://example.com"))
            .progress(Optional.of("1%"))
            .retryAfter(Optional.of(RetryValue.after(Duration.ofSeconds(10))))
            .build(),
        kickOffAcceptedResponse);
  }

  @Test
  void testProcessPoolingAcceptedResponseWithProgressAndRetryAfter() {
    final AsynResponseHandler<MyFinalResponse> handler = AsynResponseHandler.of(
        MyFinalResponse.class);
    final HttpResponse httpResponse = new BasicHttpResponse(
        HttpVersion.HTTP_1_1, 202, "Accepted");
    httpResponse.addHeader("x-progress", "50%");
    httpResponse.addHeader("retry-after", "Mon, 22 Jul 2019 23:59:59 GMT");
    final AsyncResponse acceptedResponse = handler.handleResponse(httpResponse);
    assertEquals(AcceptedAsyncResponse.builder()
            .progress(Optional.of("50%"))
            .retryAfter(Optional.of(RetryValue.at(Instant.parse("2019-07-22T23:59:59Z"))))
            .build(),
        acceptedResponse);
  }

  @Test
  void testProcessPoolingAcceptedResponseWithNoExtraHeaders() {
    final AsynResponseHandler<MyFinalResponse> handler = AsynResponseHandler.of(
        MyFinalResponse.class);
    final HttpResponse httpResponse = new BasicHttpResponse(
        HttpVersion.HTTP_1_1, 202, "Accepted");
    final AsyncResponse acceptedResponse = handler.handleResponse(httpResponse);
    assertEquals(AcceptedAsyncResponse.builder().build(),
        acceptedResponse);
  }

  @Test
  void testProcessHttpErrorStatusCodeWithOperationOutcome() {
    final AsynResponseHandler<MyFinalResponse> handler = AsynResponseHandler.of(
        MyFinalResponse.class);

    final OperationOutcome operationOutcome = OperationOutcome.builder()
        .issue(List.of(OperationOutcome.Issue.builder()
            .severity("error")
            .code("transient")
            .diagnostics("Transient error")
            .build()))
        .build();
    final HttpResponse httpResponse = new BasicHttpResponse(
        HttpVersion.HTTP_1_1, 500, "Internal Server Error");
    httpResponse.setEntity(
        new StringEntity(FhirJsonSupport.toJson(operationOutcome), ContentType.APPLICATION_JSON));
    httpResponse.addHeader("retry-after", "7");
    final HttpError ex = assertThrows(HttpError.class,
        () -> handler.handleResponse(httpResponse));
    assertEquals(500, ex.getStatusCode());
    assertEquals(Optional.of(RetryValue.after(Duration.ofSeconds(7))), ex.getRetryAfter());
    assertEquals(Optional.of(operationOutcome), ex.getOperationOutcome());
  }

  @Test
  void testProcessHttpErrorWithoutOperationOutcome() {
    final AsynResponseHandler<MyFinalResponse> handler = AsynResponseHandler.of(
        MyFinalResponse.class);

    final HttpResponse httpResponse = new BasicHttpResponse(
        HttpVersion.HTTP_1_1, 401, "Unauthorized");
    final HttpError ex = assertThrows(HttpError.class,
        () -> handler.handleResponse(httpResponse));
    assertEquals(401, ex.getStatusCode());
    assertEquals(Optional.empty(), ex.getRetryAfter());
    assertEquals(Optional.empty(), ex.getOperationOutcome());
  }
}
