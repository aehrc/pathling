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

/**
 * A resolved node in a SQL on FHIR dependency graph: either a {@link ResolvedViewDefinition} leaf
 * or a {@link ResolvedSqlView}. Each node is identified by a stable canonical key that is the basis
 * of its request-scoped temp-view name and of diamond deduplication.
 *
 * @author John Grimes
 */
public interface ResolvedDependency {

  /**
   * Returns the stable canonical identity of the resolved resource. Two references to the same
   * resource share a key, so a node is materialised only once per request, and the key cannot
   * collide with a different resource even when both are reached under the same table label.
   *
   * @return the canonical key
   */
  @Nonnull
  String getCanonicalKey();
}
