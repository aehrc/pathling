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

package au.csiro.pathling.auth;

import au.csiro.pathling.export.BulkExportException.HttpError;
import au.csiro.pathling.export.ws.AsyncResponse;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import com.google.gson.JsonParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

/**
 * Http Client ResponseHandler for the Json responses using in SMART endpoints. That include both
 * the SMART configuration discovery and the authentication endpoints. The JSON response are
 * deserialized using Gson with the `FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES` policy.
 *
 * @param <T> type of the expected final successful response.
 */
@Slf4j
class JsonResponseHandler<T> implements ResponseHandler<T> {

  private static final Gson GSON = new GsonBuilder()
      .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
      .create();

  @Nonnull
  private final Class<T> responseClass;

  JsonResponseHandler(@Nonnull final Class<T> responseClass) {
    this.responseClass = responseClass;
  }

  /**
   * Processes the response and returns the appropriate response object.
   *
   * @param response The http response to process
   * @return The appropriate {@link AsyncResponse}  object
   * @throws HttpError if an error occurs reading the response
   */
  @Override
  public T handleResponse(final HttpResponse response) throws IOException {

    final int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode == HttpStatus.SC_OK) {
      return produceResponse(response);
    } else {
      throw new ClientProtocolException("Unexpected status code: " + statusCode);
    }
  }

  private T produceResponse(@Nonnull final HttpResponse response) throws IOException {
    final String jsonBody = Optional.ofNullable(response.getEntity())
        .filter(e -> e.getContentType() != null && e.getContentType().getValue()
            .contains("application/json"))
        .flatMap(this::quietBodyAsString)
        .orElseThrow(() -> new ClientProtocolException("Response entity is not a JSON"));
    try {
      return GSON.fromJson(jsonBody, responseClass);
    } catch (final JsonParseException ex) {
      throw new ClientProtocolException("Failed to parse response body as JSON", ex);
    }
  }

  @Nonnull
  private Optional<String> quietBodyAsString(@Nonnull final HttpEntity body) {
    try {
      return Optional.of(EntityUtils.toString(body));
    } catch (final IOException ex) {
      log.warn("Failed to read response body", ex);
      return Optional.empty();
    }
  }

  /**
   * Creates a new instance of the handler.
   *
   * @param responseClass the class of the expected final successful response.
   * @param <T> type of the expected final successful response.
   * @return a new instance of the handler.
   */
  @Nonnull
  public static <T> JsonResponseHandler<T> of(
      @Nonnull final Class<T> responseClass) {
    return new JsonResponseHandler<>(responseClass);
  }
}
