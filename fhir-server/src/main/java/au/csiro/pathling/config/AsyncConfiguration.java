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

package au.csiro.pathling.config;

import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Represents configuration relating to asynchronous processing.
 */
@Data
public class AsyncConfiguration {

  /**
   * Enables asynchronous process of requests.
   */
  @NotNull
  private boolean enabled;
  
  /**
   * List of headers from exclude from the {@link HttpServerCachingConfiguration#getVary()} list for
   * the purpose of server side caching of asynchronous requests. These are likely to include
   * headers like 'Accept' or 'Accept-Encoding' which do not influence in any way the final result
   * of the async request.
   */
  @NotNull
  private List<String> varyHeadersExcludedFromCacheKey;
}
