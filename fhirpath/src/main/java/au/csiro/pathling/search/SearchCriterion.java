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

package au.csiro.pathling.search;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import lombok.Value;

/**
 * Represents a single search criterion with a parameter code, optional modifier, and one or more
 * values. Multiple values for the same parameter are combined with OR logic.
 */
@Value
public class SearchCriterion {

  /** The search parameter code (e.g., "gender"). */
  @Nonnull String parameterCode;

  /** The search modifier (e.g., "not", "exact"), or null if no modifier. */
  @Nullable String modifier;

  /** The search values. Multiple values are combined with OR logic. */
  @Nonnull List<String> values;

  /**
   * Creates a search criterion with the given parameter code and values (no modifier).
   *
   * @param parameterCode the parameter code
   * @param values the search values
   * @return the search criterion
   */
  @Nonnull
  public static SearchCriterion of(
      @Nonnull final String parameterCode, @Nonnull final String... values) {
    return new SearchCriterion(parameterCode, null, Arrays.asList(values));
  }

  /**
   * Creates a search criterion with the given parameter code and values (no modifier).
   *
   * @param parameterCode the parameter code
   * @param values the search values
   * @return the search criterion
   */
  @Nonnull
  public static SearchCriterion of(
      @Nonnull final String parameterCode, @Nonnull final List<String> values) {
    return new SearchCriterion(parameterCode, null, values);
  }

  /**
   * Creates a search criterion with the given parameter code, modifier, and values.
   *
   * @param parameterCode the parameter code
   * @param modifier the search modifier (e.g., "not", "exact"), or null
   * @param values the search values
   * @return the search criterion
   */
  @Nonnull
  public static SearchCriterion of(
      @Nonnull final String parameterCode,
      @Nullable final String modifier,
      @Nonnull final String... values) {
    return new SearchCriterion(parameterCode, modifier, Arrays.asList(values));
  }

  /**
   * Creates a search criterion with the given parameter code, modifier, and values.
   *
   * @param parameterCode the parameter code
   * @param modifier the search modifier (e.g., "not", "exact"), or null
   * @param values the search values
   * @return the search criterion
   */
  @Nonnull
  public static SearchCriterion of(
      @Nonnull final String parameterCode,
      @Nullable final String modifier,
      @Nonnull final List<String> values) {
    return new SearchCriterion(parameterCode, modifier, values);
  }
}
