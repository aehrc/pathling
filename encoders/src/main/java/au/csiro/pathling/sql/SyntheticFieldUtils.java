/*
 * Copyright 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import jakarta.annotation.Nonnull;
import lombok.experimental.UtilityClass;

/**
 * Utility methods for identifying synthetic fields added by the Pathling encoders. Synthetic fields
 * are internal metadata columns used for efficient querying and comparison, and should not appear
 * in user-facing output.
 *
 * @author John Grimes
 */
@UtilityClass
public class SyntheticFieldUtils {

  /**
   * Determines whether a field name identifies a synthetic (internal) field.
   *
   * <p>A field is synthetic if its name starts with {@code _} (e.g. {@code _fid}, {@code
   * _value_canonicalized}) or ends with {@code _scale} (e.g. {@code value_scale}).
   *
   * @param name the field name to check
   * @return {@code true} if the field is synthetic, {@code false} otherwise
   */
  public static boolean isSyntheticField(@Nonnull final String name) {
    return name.startsWith("_") || name.endsWith("_scale");
  }
}
