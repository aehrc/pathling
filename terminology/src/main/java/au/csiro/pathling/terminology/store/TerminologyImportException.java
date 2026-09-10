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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Thrown when a terminology import fails because its source is invalid: a SNOMED CT release that is
 * not a snapshot, an RF2 file with unexpected columns, or a FHIR resource that is not an importable
 * R4 CodeSystem, ValueSet, or ConceptMap. When this is thrown the store is left unmodified.
 *
 * @author John Grimes
 */
public class TerminologyImportException extends RuntimeException {

  private static final long serialVersionUID = 4470306870886711571L;

  /**
   * Creates an exception with a message.
   *
   * @param message the detail message describing the problem with the source
   */
  public TerminologyImportException(@Nonnull final String message) {
    super(message);
  }

  /**
   * Creates an exception with a message and cause.
   *
   * @param message the detail message describing the problem with the source
   * @param cause the underlying cause
   */
  public TerminologyImportException(
      @Nonnull final String message, @Nullable final Throwable cause) {
    super(message, cause);
  }
}
