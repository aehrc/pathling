/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
import lombok.Data;
import lombok.ToString;

/**
 * Configuration for the SQL query operations. Bounds the resolution of a query's dependency graph,
 * which happens before any query execution.
 *
 * @author John Grimes
 */
@Data
@ToString(doNotUseGetters = true)
public class SqlQueryConfiguration {

  /**
   * The maximum nesting depth of the dependency graph resolved for a single query. The top-level
   * query's direct {@code relatedArtifact} dependencies sit at depth one; each further level of
   * nested {@code SQLView} dependency increments the depth. A graph that nests deeper than this
   * limit is rejected before any Spark work, guarding against accidental fan-out and runaway
   * resolution. Real view graphs are shallow, so the default is generous while still bounded.
   */
  @Min(1)
  private int maxDependencyDepth = 10;
}
