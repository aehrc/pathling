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

package au.csiro.pathling.operations.bulkimport;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;

/**
 * Supported import modes for Pathling persistence schemes.
 *
 * @author John Grimes
 */
public enum ImportMode {
  /**
   * Results in all existing resources of the specified type to be deleted and replaced with the
   * contents of the source file.
   */
  OVERWRITE("overwrite"),

  /**
   * Matches existing resources with updated resources in the source file based on their ID, and
   * either update the existing resources or add new resources as appropriate.
   */
  MERGE("merge");

  @Nonnull @Getter private final String code;

  ImportMode(@Nonnull final String code) {
    this.code = code;
  }

  /**
   * Converts a code string to an ImportMode enum value.
   *
   * @param code the code string to convert
   * @return the corresponding ImportMode
   * @throws IllegalArgumentException if the code does not match any ImportMode
   */
  @Nonnull
  public static ImportMode fromCode(@Nullable final String code) {
    for (final ImportMode mode : values()) {
      if (mode.code.equals(code)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Unknown import mode: " + code);
  }
}
