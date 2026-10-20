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

package au.csiro.pathling.terminology.conceptmap;

import jakarta.annotation.Nonnull;
import java.io.Serial;

/**
 * Thrown when the version of a concept map to use cannot be determined: an unpinned reference
 * matches several versions with no determinable latest, or a pinned reference matches several
 * resources. The message carries the reason and is the contract for callers that report it.
 *
 * @author John Grimes
 */
public class ConceptMapVersionException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1830452960713588116L;

  /**
   * Creates an exception with the given reason.
   *
   * @param reason a human-readable description of why the version cannot be determined
   */
  public ConceptMapVersionException(@Nonnull final String reason) {
    super(reason);
  }

  /**
   * Creates an exception with the given reason and cause.
   *
   * @param reason a human-readable description of why the version cannot be determined
   * @param cause the underlying failure
   */
  public ConceptMapVersionException(@Nonnull final String reason, @Nonnull final Throwable cause) {
    super(reason, cause);
  }
}
