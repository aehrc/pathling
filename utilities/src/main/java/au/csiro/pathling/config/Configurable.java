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

package au.csiro.pathling.config;

import jakarta.annotation.Nonnull;

/**
 * Interface for objects that can provide access to their configuration.
 *
 * <p>This interface establishes a consistent pattern for accessing configuration objects from
 * service instances. Implementations may either store the configuration internally or construct it
 * on-demand from their current state.
 *
 * @param <T> the type of configuration object provided by this instance
 */
public interface Configurable<T> {

  /**
   * Returns the configuration object associated with this instance.
   *
   * <p>Implementations may either return a stored configuration object or construct one on-demand
   * from the current state of the instance.
   *
   * @return the configuration object, never null
   */
  @Nonnull
  T getConfiguration();
}
