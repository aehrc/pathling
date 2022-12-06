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

package au.csiro.pathling.config;

import java.io.Serializable;
import javax.validation.constraints.Min;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents configuration relating to the HTTP client.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HttpClientConfiguration implements Serializable {

  private static final long serialVersionUID = -1624276800166930462L;

  public static final int DEFAULT_MAX_CONNECTIONS_TOTAL = 32;
  public static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 16;
  public static final int DEFAULT_SOCKET_TIMEOUT = 60_000;

  /**
   * The maximum total number of connections for the client.
   * <p>
   * See also: {@link org.apache.http.impl.client.HttpClientBuilder#setMaxConnTotal(int)}
   */
  @Min(0)
  @Builder.Default
  private int maxConnectionsTotal = DEFAULT_MAX_CONNECTIONS_TOTAL;

  /**
   * The maximum number of connections per route for the client.
   * <p>
   * See also: {@link org.apache.http.impl.client.HttpClientBuilder#setMaxConnPerRoute(int)}
   */
  @Min(0)
  @Builder.Default
  private int maxConnectionsPerRoute = DEFAULT_MAX_CONNECTIONS_PER_ROUTE;

  /**
   * The maximum period (in milliseconds) that the server should wait for incoming data from the
   * HTTP service.
   */
  @Min(0)
  @Builder.Default
  private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;

  public static HttpClientConfiguration defaults() {
    return HttpClientConfiguration.builder().build();
  }

}
