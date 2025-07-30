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

package au.csiro.pathling.projection;

import jakarta.annotation.Nonnull;

/**
 * Represents a component of an abstract query for projecting FHIR data.
 */
public interface ProjectionClause {

  /**
   * Converts this clause into a result using the supplied context.
   *
   * @param context The context in which to evaluate this clause
   * @return The result of evaluating this clause
   */
  @Nonnull
  ProjectionResult evaluate(@Nonnull final ProjectionContext context);

  /**
   * Returns a tree-like string representation of this clause for debugging purposes.
   *
   * @param level the indentation level for the tree structure
   * @return a formatted tree string representation
   */
  @Nonnull
  String toTreeString(final int level);

}
