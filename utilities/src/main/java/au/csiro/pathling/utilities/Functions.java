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

package au.csiro.pathling.utilities;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Utility class containing some functional programming helper functions. */
public interface Functions {

  /**
   * Converts a function that takes two arguments to its curried version.
   *
   * @param f the function to be curried
   * @param <A1> the first argument type
   * @param <A2> the second argument type
   * @param <R> the return type
   * @return the curried function
   */
  static <A1, A2, R> Function<A1, Function<A2, R>> curried(final BiFunction<A1, A2, R> f) {
    return a1 -> a2 -> f.apply(a1, a2);
  }

  /**
   * Converts a function that takes two arguments to its curried version with reversed arguments.
   *
   * @param f the function to be curried
   * @param <A1> the first argument type
   * @param <A2> the second argument type
   * @param <R> the return type
   * @return the curried function
   */
  static <A1, A2, R> Function<A2, Function<A1, R>> backCurried(final BiFunction<A1, A2, R> f) {
    return a2 -> a1 -> f.apply(a1, a2);
  }

  /**
   * Returns function that conditionally casts an object to a given class, returning an empty
   * optional if the cast fails.
   *
   * @param clazz the class to cast to
   * @param <T> the class to cast to
   * @return the function
   */
  static <T> Function<Object, Optional<T>> maybeCast(final Class<T> clazz) {
    return o -> clazz.isInstance(o) ? Optional.of(clazz.cast(o)) : Optional.empty();
  }
}
