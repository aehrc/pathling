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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;

/** Names a document in a failure by its first characters, where nothing better names it. */
final class DocumentExcerpt {

  /** How many characters of a document name it. */
  private static final int LENGTH = 80;

  private DocumentExcerpt() {}

  /**
   * Returns the phrase that names a document by its first characters, quoted, and shortened with an
   * ellipsis where the document is longer.
   *
   * @param document the document
   * @return the phrase
   */
  @Nonnull
  static String startOf(@Nonnull final String document) {
    final String start = document.strip();
    return "starting \""
        + (start.length() > LENGTH ? start.substring(0, LENGTH) + "…" : start)
        + "\"";
  }
}
