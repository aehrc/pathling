/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import static java.util.Objects.isNull;

import jakarta.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a default value of type T. Simplifies resolution of values where null represents the
 * default, e.g:
 * <pre>{@code
 *    Default<Integer> DEFAULT_VALUE = Default.of(100);
 *    ...
 *    int value = DEFAULT_VALUE.resolve(valueOrNull);
 * }</pre>
 *
 * @param <T> the type of the value.
 */

public class Default<T> {

  @Nonnull
  private final T defaultValue;

  private Default(@Nonnull final T defaultValue) {
    this.defaultValue = defaultValue;
  }

  /**
   * Resolved a nullable value to the value itself it not null or otherwise to the underlying
   * default value.
   *
   * @param value the nullable value to resolve
   * @return the value of the default
   */
  @Nonnull
  public T resolve(@Nullable T value) {
    return isNull(value)
           ? defaultValue
           : value;
  }

  /**
   * Constructs the default value for given type T.
   *
   * @param defaultValue the default value.
   * @param <T> the type of the value.
   * @return the default object.
   */
  @Nonnull
  public static <T> Default<T> of(@Nonnull final T defaultValue) {
    return new Default<>(defaultValue);
  }

}
