/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.io.Serial;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * Represents configuration relating to the HTTP client used for terminology requests.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Data
@Builder
public class HttpClientConfiguration implements Serializable {

  @Serial private static final long serialVersionUID = -1624276800166930462L;

  /**
   * The maximum period (in milliseconds) that the server should wait for incoming data from the
   * HTTP service.
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private int socketTimeout = 60_000;

  /**
   * The maximum total number of connections for the client.
   *
   * @see org.apache.http.impl.client.HttpClientBuilder#setMaxConnTotal
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private int maxConnectionsTotal = 32;

  /**
   * The maximum number of connections per route for the client.
   *
   * @see org.apache.http.impl.client.HttpClientBuilder#setMaxConnPerRoute
   */
  @Min(0)
  @Builder.Default
  private int maxConnectionsPerRoute = 16;

  /**
   * Controls whether terminology requests that fail for possibly transient reasons (network
   * connections, DNS problems) should be retried.
   */
  @NotNull @Builder.Default private boolean retryEnabled = true;

  /** The number of times to retry failed terminology requests. */
  @NotNull
  @Min(1)
  @Builder.Default
  private int retryCount = 2;
}
