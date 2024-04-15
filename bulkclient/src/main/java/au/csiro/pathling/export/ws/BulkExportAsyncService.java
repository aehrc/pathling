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

import java.io.IOException;
import java.net.URI;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;


/**
 * Service for handling asynchronous requests for bulk export.
 */
@AllArgsConstructor
@Slf4j
public class BulkExportAsyncService {

  @Nonnull
  final HttpClient httpClient;

  @Nonnull
  final URI fhirEndpointUri;

  /**
   * Kicks off a bulk export request.
   *
   * @param request the request to kick off
   * @return the {@link  AsyncResponse}response
   * @throws IOException if an error occurs during the request
   */
  @Nonnull
  AsyncResponse kickOff(@Nonnull final BulkExportRequest request) throws IOException {
    final HttpUriRequest httpRequest = request.toHttpRequest(fhirEndpointUri);
    log.debug("KickOff: Request: {}", httpRequest);
    if (httpRequest instanceof HttpPost) {
      log.debug("KickOff: Request body: {}",
          EntityUtils.toString(((HttpPost) httpRequest).getEntity()));
    }
    return httpClient.execute(httpRequest, AsynResponseHandler.of(BulkExportResponse.class));
  }

  /**
   * Checks the status of a bulk export request. Returns an {@link AsyncResponse} that may contain a
   * {@link AcceptedAsyncResponse} or a {@link BulkExportResponse}  on completion.
   *
   * @param statusUri the status URI
   * @return the {@link AsyncResponse} response
   * @throws IOException if an error occurs during the request
   */
  @Nonnull
  AsyncResponse checkStatus(@Nonnull final URI statusUri) throws IOException {
    log.debug("Pooling: Get status from: " + statusUri);
    final HttpUriRequest statusRequest = new HttpGet(statusUri);
    statusRequest.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
    return httpClient.execute(statusRequest, AsynResponseHandler.of(BulkExportResponse.class));
  }

  /**
   * Cancels a bulk export request.
   *
   * @param statusUri the status URI
   * @throws IOException if an error occurs during the request
   */
  void cleanup(@Nonnull final URI statusUri) throws IOException {
    log.debug("Cleanup: Deleting: " + statusUri);
    final HttpUriRequest statusRequest = new HttpDelete(statusUri);
    statusRequest.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
    final HttpResponse response = httpClient.execute(statusRequest);
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != HttpStatus.SC_ACCEPTED) {
      log.warn("Failed to cleanup: {}. Status code: {}", statusUri, statusCode);
    }
  }
}
