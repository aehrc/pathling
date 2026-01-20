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

package au.csiro.pathling.fhirpath;

import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;

/** Helper functions for codings. */
public interface CodingHelpers {

  /**
   * Tests for coding equality using only their systems, codes and versions. Two codings are equal
   * if both their systems and codes are equal and either the versions are equal or at least one
   * version is null.
   *
   * @param left the left coding.
   * @param right the right coding.
   * @return if codings satisfy the equality criteria above.
   */
  static boolean codingEquals(@Nullable final Coding left, @Nullable final Coding right) {
    if (left == null) {
      return right == null;
    } else {

      return right != null
          && (left.hasSystem() ? left.getSystem().equals(right.getSystem()) : !right.hasSystem())
          && (left.hasCode() ? left.getCode().equals(right.getCode()) : !right.hasCode())
          && (!left.hasVersion()
              || !right.hasVersion()
              || left.getVersion().equals(right.getVersion()));
    }
  }
}
