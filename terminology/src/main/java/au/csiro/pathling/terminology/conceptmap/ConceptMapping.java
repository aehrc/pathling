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

package au.csiro.pathling.terminology.conceptmap;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import lombok.Value;

/**
 * One row of a concept map relation: a source code with its system, version and display, and a
 * target code with its system, version, display and relationship, or a null target where the map
 * states that the source code has no mapping.
 *
 * <p>A mapping is identified by every field except the two displays, with two null values equal.
 * Two mappings with the same identity are the same mapping; the first occurrence's displays are
 * kept when a concept map is deduplicated.
 *
 * @author John Grimes
 */
@Value
public class ConceptMapping implements Serializable {

  @Serial private static final long serialVersionUID = 2932181154795130741L;

  /** Separates the parts of an identity key. */
  private static final char SEPARATOR = '\u0000';

  /** Stands in for a null value within an identity key, so that it differs from any string. */
  private static final String NULL_VALUE = "\u0001";

  /** The code system of the source code. */
  @Nonnull String sourceSystem;

  /** The code system version the mapping was authored against, or null where none is recorded. */
  @Nullable String sourceVersion;

  /** The source code. */
  @Nonnull String sourceCode;

  /** The display of the source code, or null where the map gives none. */
  @Nullable String sourceDisplay;

  /** The code system of the target code, or null where the group names no target. */
  @Nullable String targetSystem;

  /** The target code system version, or null where the group records none. */
  @Nullable String targetVersion;

  /** The target code, or null on a no-mapping row. */
  @Nullable String targetCode;

  /** The display of the target code, or null where the map gives none or on a no-mapping row. */
  @Nullable String targetDisplay;

  /** An R5 ConceptMapRelationship code, or null on a no-mapping row. */
  @Nullable String relationship;

  /**
   * Returns a key that is equal for two mappings with the same identity, treating two null values
   * as equal and a null value as distinct from every non-null value.
   *
   * @return the identity key
   */
  @Nonnull
  public String identity() {
    return sourceSystem
        + SEPARATOR
        + orMarker(sourceVersion)
        + SEPARATOR
        + sourceCode
        + SEPARATOR
        + orMarker(targetSystem)
        + SEPARATOR
        + orMarker(targetVersion)
        + SEPARATOR
        + orMarker(targetCode)
        + SEPARATOR
        + orMarker(relationship);
  }

  @Nonnull
  private static String orMarker(@Nullable final String value) {
    return value == null ? NULL_VALUE : value;
  }
}
