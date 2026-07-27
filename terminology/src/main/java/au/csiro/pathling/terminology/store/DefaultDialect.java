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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;

/**
 * Chooses the language reference set whose preferred synonyms become the stored display of every
 * concept in a release.
 *
 * @author John Grimes
 */
public final class DefaultDialect {

  private DefaultDialect() {
    // Utility class.
  }

  /**
   * Chooses the default language reference set for a release.
   *
   * @param requested the value of the import option, or null to derive the choice
   * @param editionModule the detected edition module of the release, or null if unknown
   * @param candidatesByName the language reference sets the release holds, keyed by identifier and
   *     valued by the fully specified name the release gives that concept, which may be null
   * @return the chosen reference set identifier, or null when the release holds none
   * @throws TerminologyImportException if the request cannot be honoured, or the release holds
   *     several reference sets and no rule selects one
   */
  @Nullable
  public static String choose(
      @Nullable final String requested,
      @Nullable final String editionModule,
      @Nonnull final Map<String, String> candidatesByName) {
    return null;
  }
}
