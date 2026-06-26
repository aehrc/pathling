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

package au.csiro.pathling.operations.sqlquery;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import lombok.Value;

/**
 * The fully resolved dependency graph for a single query: the transitive set of {@link
 * ResolvedDependency} nodes reachable from the top-level query, together with the mapping from the
 * top-level query's own table labels to the nodes they reference. Produced during request
 * preparation (no Spark) and materialised, bottom-up, at execution.
 *
 * @author John Grimes
 */
@Value
public class ResolvedDependencyGraph {

  /**
   * The nodes in topological order: every node appears after all of its dependencies, so
   * materialising the list in order guarantees each node's children already exist as temp views.
   */
  @Nonnull List<ResolvedDependency> orderedNodes;

  /** The top-level query's local table label to the canonical key of the node it references. */
  @Nonnull Map<String, String> topLevelKeysByLabel;

  /** Lookup of every node by its canonical key, for deduplication and materialisation. */
  @Nonnull Map<String, ResolvedDependency> nodesByKey;
}
