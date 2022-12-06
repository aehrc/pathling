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

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.hibernate.validator.constraints.URL;
import org.springframework.boot.context.properties.DeprecatedConfigurationProperty;

/**
 * Represents configuration specific to the terminology functions of the server.
 */
@Data
public class TerminologyConfiguration {

  /**
   * Enables the use of terminology functions.
   */
  @NotNull
  private boolean enabled;

  /**
   * The endpoint of a FHIR terminology service (R4) that the server can use to resolve terminology
   * queries.
   */
  @NotBlank
  @URL
  private String serverUrl;


  /**
   * The maximum period (in milliseconds) that the server should wait for incoming data from the
   * terminology service.
   */
  @Nullable
  @Min(0)
  private Integer socketTimeout;

  @DeprecatedConfigurationProperty(replacement = "client.socketTimeout")
  @Nullable
  public Integer getSocketTimeout() {
    return socketTimeout;
  }

  /**
   * Setting this option to {@code true} will enable additional logging of the details of requests
   * between the server and the terminology service.
   */
  @NotNull
  private boolean verboseLogging;

  @NotNull
  private HttpClientConfiguration client;

  @NotNull
  private HttpCacheConfiguration cache;

  @NotNull
  private TerminologyAuthConfiguration authentication;

}
