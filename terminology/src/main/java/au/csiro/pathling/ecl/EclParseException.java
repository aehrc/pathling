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

package au.csiro.pathling.ecl;

import jakarta.annotation.Nonnull;

/**
 * Thrown when an ECL expression is malformed and cannot be parsed. The message identifies the
 * position in the expression and the reason for the failure.
 *
 * @author John Grimes
 */
public class EclParseException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a parse exception.
   *
   * @param position the one-based character position of the error
   * @param reason the reason for the parse failure
   */
  public EclParseException(final int position, @Nonnull final String reason) {
    super("Invalid ECL expression at position " + position + ": " + reason);
  }
}
