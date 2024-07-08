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

package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Utility class containing some List helper functions.
 *
 * @author Piotr Szul
 */
public abstract class Lists {

  /**
   * Returns an empty list if the input list is empty; otherwise, returns the input list.
   *
   * @param list the list to normalize
   * @param <T> the type of elements in the list
   * @return an empty list if the input list is empty; otherwise, the input list
   */
  @Nonnull
  public static <T> List<T> normalizeEmpty(@Nonnull final List<T> list) {
    return list.isEmpty()
           ? Collections.emptyList()
           : list;
  }


  /**
   * Returns an empty list if the input list is not present; otherwise, returns the input list.
   *
   * @param maybeList the list to normalize
   * @return an empty list if the input list is not present; otherwise, the input list
   */
  @Nonnull
  public static List<String> normalizeEmpty(@Nonnull final Optional<List<String>> maybeList) {
    return maybeList.orElse(Collections.emptyList());
  }


  /**
   * Returns an optional of given list for non-empty list, or empty optional otherwise.
   *
   * @param list list to convert
   * @param <T> type of list elements
   * @return option of given list for non-empty list, or empty option otherwise
   */
  @Nonnull
  public static <T> Optional<List<T>> optionalOf(@Nonnull final List<T> list) {
    return list.isEmpty()
           ? Optional.empty()
           : Optional.of(list);
  }

}
