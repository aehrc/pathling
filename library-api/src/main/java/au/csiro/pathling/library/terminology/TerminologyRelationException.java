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

package au.csiro.pathling.library.terminology;

import jakarta.annotation.Nonnull;
import java.io.Serial;

/**
 * Thrown when a value set or concept map cannot be resolved to a single content, so that no
 * relation can be built for it: the canonical URL cannot be resolved, the version to use cannot be
 * determined, the membership cannot be determined, the concept map carries content that a relation
 * cannot represent, or the content exceeds the caller's cap. The message names the artefact and
 * carries the reason.
 *
 * @author John Grimes
 */
public class TerminologyRelationException extends RuntimeException {

  @Serial private static final long serialVersionUID = 3170962264013458201L;

  /**
   * Creates an exception with the given message.
   *
   * @param message a description naming the artefact that could not be resolved
   */
  public TerminologyRelationException(@Nonnull final String message) {
    super(message);
  }

  /**
   * Creates an exception with the given message and cause.
   *
   * @param message a description naming the artefact that could not be resolved, and why
   * @param cause the underlying failure
   */
  public TerminologyRelationException(
      @Nonnull final String message, @Nonnull final Throwable cause) {
    super(message, cause);
  }
}
