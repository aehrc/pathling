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

package au.csiro.pathling.library.io;

import jakarta.annotation.Nullable;
import java.io.Serial;

/**
 * Represents an error that occurs during data persistence operations.
 *
 * @author John Grimes
 */
public class PersistenceError extends RuntimeException {

  @Serial private static final long serialVersionUID = -757932366975899363L;

  /**
   * Creates a new PersistenceError.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public PersistenceError(final String message, @Nullable final Throwable cause) {
    super(message, cause);
  }
}
