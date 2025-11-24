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

package au.csiro.pathling.terminology;

import au.csiro.pathling.config.Configurable;
import au.csiro.pathling.config.TerminologyConfiguration;
import jakarta.annotation.Nonnull;
import java.io.Serializable;

/**
 * Represents something that creates a {@link TerminologyService}.
 * <p>
 * Used for code that runs on Spark workers, providing a {@link Serializable} class that is capable
 * of building a service that can be used on the worker.
 * <p>
 * Implementations that support configuration access should override
 * {@link #getConfiguration()} to return their terminology configuration.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public interface TerminologyServiceFactory extends Serializable,
    Configurable<TerminologyConfiguration> {

  /**
   * Builds a new instance.
   *
   * @return a shiny new TerminologyService instance
   */
  @Nonnull
  TerminologyService build();

  /**
   * Returns the terminology configuration used by this factory.
   * <p>
   * Default implementation throws {@link IllegalStateException}. Production implementations should
   * override this method to return their configuration.
   *
   * @return the terminology configuration, never null
   * @throws IllegalStateException if this factory does not support configuration access
   */
  @Nonnull
  @Override
  default TerminologyConfiguration getConfiguration() {
    throw new IllegalStateException(
        this.getClass().getName() + " does not support configuration access");
  }
}
