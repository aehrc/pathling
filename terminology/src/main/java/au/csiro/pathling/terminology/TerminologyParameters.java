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

package au.csiro.pathling.terminology;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serializable;
import java.util.function.Function;

/**
 * Represents parameters that are passed to a terminology operation.
 * <p>
 * Must be {@link Serializable} so that it can be used as a cache key.
 *
 * @author John Grimes
 */
public interface TerminologyParameters extends Serializable {

  /**
   * Converts an optional string value using the provided converter.
   *
   * @param <T> the type to convert to
   * @param converter the function to convert the string value
   * @param value the string value to convert, may be null
   * @return the converted value, or null if input is null
   */
  @Nullable
  static <T> T optional(@Nonnull final Function<String, T> converter,
      @Nullable final String value) {
    return value != null
           ? converter.apply(value)
           : null;
  }

  /**
   * Converts a required string value using the provided converter.
   *
   * @param <T> the type to convert to
   * @param converter the function to convert the string value
   * @param value the string value to convert, must not be null
   * @return the converted value
   * @throws NullPointerException if value is null
   */
  @Nonnull
  static <T> T required(@Nonnull final Function<String, T> converter,
      @Nullable final String value) {
    return converter.apply(requireNonNull(value));
  }

}
