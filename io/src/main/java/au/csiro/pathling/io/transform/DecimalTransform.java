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
package au.csiro.pathling.io.transform;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.Column;

/**
 * The storage of a decimal, which is the lexical form of the source as text (FR-002).
 *
 * <p>Nothing on this path may parse the value as a number. A skeleton, pending implementation.
 */
public final class DecimalTransform {

  private DecimalTransform() {}

  /**
   * Returns the options a JSON read needs in order to deliver a decimal as the text of the source.
   *
   * @return the read options
   */
  @Nonnull
  public static Map<String, String> lexicalReadOptions() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns the stored value of a decimal, given the column the source was read into.
   *
   * @param source the column the source was read into
   * @return the stored value
   */
  @Nonnull
  public static Column storedValue(@Nonnull final Column source) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
