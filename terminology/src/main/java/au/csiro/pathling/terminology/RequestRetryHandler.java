/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;

/**
 * Overrides the default retry handler to retry on a wider range of exceptions.
 *
 * @author John Grimes
 */
@Slf4j
public class RequestRetryHandler extends DefaultHttpRequestRetryHandler {

  public RequestRetryHandler() {
    super(2, true,
        List.of(InterruptedIOException.class, UnknownHostException.class, ConnectException.class,
            SSLException.class, SocketException.class, HttpHostConnectException.class,
            ConnectTimeoutException.class, NoHttpResponseException.class));
  }

  @Override
  public boolean retryRequest(final IOException exception, final int executionCount,
      final HttpContext context) {
    log.debug("Problem issuing terminology request, retrying", exception);
    return super.retryRequest(exception, executionCount, context);
  }
}
