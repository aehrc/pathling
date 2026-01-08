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

package au.csiro.pathling.search.filter;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * A function that matches a single Column element against a search value.
 * <p>
 * Implementations define the matching logic for different search parameter types (e.g., token,
 * string). The matching logic is independent of array/scalar handling, which is managed by
 * {@link SearchFilter}.
 */
@FunctionalInterface
public interface ElementMatcher {

  /**
   * Returns a boolean Column expression that matches the element against the search value.
   *
   * @param element the Column representing a single element value
   * @param searchValue the search value to match against
   * @return a Column expression that evaluates to true if the element matches
   */
  @Nonnull
  Column match(@Nonnull Column element, @Nonnull String searchValue);
}
