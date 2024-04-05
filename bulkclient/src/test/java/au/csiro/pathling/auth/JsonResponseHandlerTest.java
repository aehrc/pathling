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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.Value;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.jupiter.api.Test;

class JsonResponseHandlerTest {


  @Value
  static class TestResponse {

    @Nonnull
    String stringValue;
  }

  @Test
  void testProducesObjectReponseForValiddHttpResponseWithJsonBody() throws IOException {
    final BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
    response.setEntity(
        new StringEntity("{\"string_value\":\"abc\"}", ContentType.APPLICATION_JSON));
    assertEquals(new TestResponse("abc"),
        JsonResponseHandler.of(TestResponse.class).handleResponse(response));
  }

  @Test
  void testThrowsExceptionForHttpError() {
    final BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 400,
        "Bad Request");
    final IOException ex = assertThrows(IOException.class,
        () -> JsonResponseHandler.of(TestResponse.class).handleResponse(response));
    assertEquals("Unexpected status code: 400", ex.getMessage());
  }

  @Test
  void testThrowsProtocolExceptionWhenNoBodyInResponse() {
    final BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
    final ClientProtocolException ex = assertThrows(ClientProtocolException.class,
        () -> JsonResponseHandler.of(TestResponse.class).handleResponse(response));
    assertEquals("Response entity is not a JSON", ex.getMessage());
  }

  @Test
  void testThrowsProtocolExceptionWhenNonJsonResponse() {
    final BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
    response.setEntity(new StringEntity("{\"string_value\":\"abc\"}", ContentType.TEXT_PLAIN));
    final ClientProtocolException ex = assertThrows(ClientProtocolException.class,
        () -> JsonResponseHandler.of(TestResponse.class).handleResponse(response));
    assertEquals("Response entity is not a JSON", ex.getMessage());
  }


  @Test
  void testThrowsProtocolExceptionWheInvalidJsonResponse() {
    final BasicHttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
    response.setEntity(new StringEntity("invalidJSON", ContentType.APPLICATION_JSON));
    final ClientProtocolException ex = assertThrows(ClientProtocolException.class,
        () -> JsonResponseHandler.of(TestResponse.class).handleResponse(response));
    assertEquals("Failed to parse response body as JSON", ex.getMessage());
  }
}
