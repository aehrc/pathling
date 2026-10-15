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
package au.csiro.pathling.legacy;

import jakarta.annotation.Nonnull;
import java.io.Serial;

/**
 * Thrown where a legacy export cannot be made: the source is not a legacy-layout table, cannot be
 * read, or holds columns the legacy encoder would not read, or the output already holds the
 * resource type.
 *
 * @author Piotr Szul
 */
public final class LegacyExportException extends RuntimeException {

  @Serial private static final long serialVersionUID = 6153077447452436017L;

  /**
   * Creates the exception.
   *
   * @param message what went wrong and, where there is one, the remedy
   */
  public LegacyExportException(@Nonnull final String message) {
    super(message);
  }

  /**
   * Creates the exception, with its cause.
   *
   * @param message what went wrong and, where there is one, the remedy
   * @param cause the cause
   */
  public LegacyExportException(@Nonnull final String message, @Nonnull final Throwable cause) {
    super(message, cause);
  }
}
