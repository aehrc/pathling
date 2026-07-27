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

package au.csiro.pathling.terminology.local;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * Parses a weighted language preference list into the tags to try, in the order to try them.
 *
 * @author John Grimes
 */
public final class LanguagePreference {

  private LanguagePreference() {
    // Utility class.
  }

  /**
   * Parses a language preference header value into an ordered list of tags.
   *
   * @param headerValue the header value, which may be null, blank or malformed
   * @return the tags to try, in descending order of weight; empty when no preference is expressed
   */
  @Nonnull
  public static List<String> parse(@Nullable final String headerValue) {
    return List.of();
  }
}
