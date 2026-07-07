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

package au.csiro.pathling.vcl;

import jakarta.annotation.Nonnull;
import java.io.Serial;

/**
 * Thrown when a VCL expression cannot be parsed. The message reports the position (character
 * offset, one-based) and reason of the failure.
 *
 * @author John Grimes
 */
public class VclParseException extends RuntimeException {

  @Serial private static final long serialVersionUID = -6821330066417583746L;

  private final int position;

  /**
   * Creates a new exception.
   *
   * @param position the one-based character position at which parsing failed
   * @param reason a description of the failure
   */
  public VclParseException(final int position, @Nonnull final String reason) {
    super("Invalid VCL expression at position " + position + ": " + reason);
    this.position = position;
  }

  /**
   * Returns the one-based character position at which parsing failed.
   *
   * @return the failure position
   */
  public int getPosition() {
    return position;
  }
}
