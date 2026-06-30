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
import jakarta.annotation.Nullable;

/**
 * One {@code query} repetition of a {@code $sqlquery-export} request, prepared (parsed, parameter
 * bound, and view resolved) at kick-off and carried to background execution. Each query input
 * produces exactly one export output.
 *
 * @param name the optional {@code query.name}, the highest-precedence output name
 * @param libraryName the SQLQuery Library's {@code name} element, used as the output-name fallback
 * @param preparedQuery the prepared query (parsed SQL, bound parameters, resolved views)
 * @author John Grimes
 */
public record QueryInput(
    @Nullable String name, @Nullable String libraryName, @Nonnull PreparedSqlQuery preparedQuery) {

  /**
   * Derives the output name for this query: the {@code query.name} when supplied, otherwise the
   * SQLQuery Library's {@code name} element, otherwise a generated name based on the index. Names
   * are made unique across the export by the executor.
   *
   * @param index the index of this query in the request (used for the generated fallback)
   * @return the effective output name before uniqueness is applied
   */
  @Nonnull
  public String getEffectiveName(final int index) {
    if (name != null && !name.isBlank()) {
      return name;
    }
    if (libraryName != null && !libraryName.isBlank()) {
      return libraryName;
    }
    return "query_" + index;
  }
}
