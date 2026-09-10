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
 * Thrown when an ECL expression is syntactically valid but uses a construct outside the subset that
 * Pathling's local terminology engine supports. The message names the offending construct so the
 * caller can tell exactly why the query was rejected, rather than receiving silently wrong results.
 *
 * @author John Grimes
 */
public class UnsupportedEclConstructError extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates an error naming the unsupported construct.
   *
   * @param construct a description of the construct that is not supported
   */
  public UnsupportedEclConstructError(@Nonnull final String construct) {
    super("Unsupported ECL construct: " + construct);
  }
}
