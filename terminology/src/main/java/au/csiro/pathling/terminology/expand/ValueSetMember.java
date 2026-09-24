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

package au.csiro.pathling.terminology.expand;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import lombok.Value;

/**
 * One member of an expanded value set.
 *
 * <p>A member is identified by its {@code system}, {@code version} and {@code code}; two members
 * with the same identity are the same member, with a null version equal only to another null
 * version. {@code display} and {@code inactive} are informative and take no part in identity.
 *
 * @author John Grimes
 */
@Value
public class ValueSetMember implements Serializable {

  @Serial private static final long serialVersionUID = 4185021771839296135L;

  /** Separates the parts of an identity key. */
  private static final char SEPARATOR = '\u0000';

  /** Stands in for a null version within an identity key, so that it differs from any string. */
  private static final String NULL_VERSION = "\u0001";

  /** The canonical URL of the code system the member belongs to. */
  @Nonnull String system;

  /** The code system version used to determine membership, or null where none was recorded. */
  @Nullable String version;

  /** The member code. */
  @Nonnull String code;

  /** The display text of the member, or null where the source provides none. */
  @Nullable String display;

  /**
   * Whether the member is inactive: {@code true} where it is inactive, {@code false} only where the
   * source explicitly says so, and null otherwise.
   */
  @Nullable Boolean inactive;

  /**
   * Returns a key that is equal for two members with the same {@code system}, {@code version} and
   * {@code code}, treating two null versions as equal and a null version as distinct from every
   * non-null version.
   *
   * @return the identity key
   */
  @Nonnull
  public String identity() {
    return system + SEPARATOR + (version == null ? NULL_VERSION : version) + SEPARATOR + code;
  }
}
