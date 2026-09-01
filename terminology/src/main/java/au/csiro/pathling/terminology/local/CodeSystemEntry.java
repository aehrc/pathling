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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CODE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

/**
 * A single imported code system version, as recorded in the store's {@code code_system} table. The
 * catalogue of these entries drives value set resolution and version selection.
 *
 * @author John Grimes
 */
@Value
public class CodeSystemEntry {

  /** The canonical URL of the code system. */
  @Nonnull String url;

  /** The version of the code system, or null if unversioned. */
  @Nullable String version;

  /** The stable identifier of this code system version within the store. */
  @Nonnull String systemVersionId;

  /**
   * Loads the catalogue of code system versions from the store.
   *
   * @param reader the store reader
   * @return the code system entries
   */
  @Nonnull
  public static List<CodeSystemEntry> loadCatalogue(@Nonnull final TerminologyStoreReader reader) {
    final List<CodeSystemEntry> entries = new ArrayList<>();
    reader.readTable(
        CODE_SYSTEM,
        row ->
            entries.add(
                new CodeSystemEntry(
                    row.getString(COLUMN_URL),
                    row.getString(COLUMN_VERSION),
                    row.getString(COLUMN_SYSTEM_VERSION_ID))));
    return entries;
  }
}
