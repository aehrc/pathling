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

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

/**
 * Represents configuration that controls the behaviour of query executors.
 *
 * @author Piotr Szul
 */
@Data
@Builder
public class QueryConfiguration {

  /**
   * Setting this option to {@code true} will enable additional logging relating to the query plan
   * used to execute queries.
   */
  @NotNull
  @Builder.Default
  private Boolean explainQueries = false;

  /**
   * This controls whether the built-in caching within Spark is used for search results. It may be
   * useful to turn this off for large datasets in memory-constrained environments.
   */
  @NotNull
  @Builder.Default
  private Boolean cacheResults = true;
}
