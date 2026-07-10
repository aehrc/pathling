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

package au.csiro.pathling.config;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;

/**
 * Selects the backend used to evaluate the terminology functions.
 *
 * @author John Grimes
 */
public enum TerminologyMode {
  /** Terminology operations are resolved against a remote FHIR terminology server. */
  SERVER("server"),

  /** Terminology operations are resolved against a local, imported terminology store. */
  LOCAL("local");

  @Nonnull private final String code;

  TerminologyMode(@Nonnull final String code) {
    this.code = requireNonNull(code);
  }

  @Override
  public String toString() {
    return code;
  }

  /**
   * Returns the terminology mode corresponding to the given code.
   *
   * @param code the code of the mode (for example {@code "server"} or {@code "local"})
   * @return the {@link TerminologyMode} corresponding to the given code
   * @throws IllegalArgumentException if the code does not correspond to a known mode
   */
  @Nonnull
  public static TerminologyMode fromCode(@Nonnull final String code) {
    for (final TerminologyMode mode : values()) {
      if (mode.code.equals(code)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Unknown terminology mode: " + code);
  }
}
