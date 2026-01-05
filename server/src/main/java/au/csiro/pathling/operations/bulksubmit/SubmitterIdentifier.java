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

package au.csiro.pathling.operations.bulksubmit;

import jakarta.annotation.Nonnull;

/**
 * Represents a submitter identifier with system and value, following FHIR Identifier structure.
 *
 * @param system The namespace for the identifier value.
 * @param value The value of the identifier.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record SubmitterIdentifier(@Nonnull String system, @Nonnull String value) {

  /**
   * Returns a string key suitable for use in maps and registries.
   *
   * @return A composite key combining system and value.
   */
  @Nonnull
  public String toKey() {
    return system + "|" + value;
  }
}
