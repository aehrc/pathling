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

package au.csiro.pathling.terminology.local.index;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ACCEPTABILITY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_LANGUAGE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TERM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.DESCRIPTION;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The description index for one code system version: the terms of each concept with their language,
 * type, and (for SNOMED) language-reference-set acceptability. It backs the display, designation,
 * and property lookup operations, and is loaded only when one of those operations is first used.
 *
 * @author John Grimes
 */
public final class DescriptionIndex {

  @Nonnull private final Map<Integer, List<Description>> byConcept;

  private DescriptionIndex(@Nonnull final Map<Integer, List<Description>> byConcept) {
    this.byConcept = byConcept;
  }

  /**
   * Loads the description index for a code system version.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the loaded index
   */
  @Nonnull
  public static DescriptionIndex load(
      @Nonnull final TerminologyStoreReader reader, @Nonnull final String systemVersionId) {
    final Map<Integer, List<Description>> byConcept = new HashMap<>();
    reader.readTable(
        DESCRIPTION,
        row -> {
          if (!systemVersionId.equals(row.getString(COLUMN_SYSTEM_VERSION_ID))) {
            return;
          }
          final String term = row.getString(COLUMN_TERM);
          if (term == null) {
            return;
          }
          byConcept
              .computeIfAbsent(row.getInt(COLUMN_CONCEPT_DENSE_ID), k -> new ArrayList<>())
              .add(
                  new Description(
                      term,
                      row.getString(COLUMN_LANGUAGE),
                      row.getString(COLUMN_TYPE_CODE),
                      row.getString(COLUMN_TYPE_SYSTEM),
                      row.getStringMap(COLUMN_ACCEPTABILITY)));
        });
    // Ordering each concept's descriptions here, rather than at each of the several places they are
    // read, removes the dependency on store row order in one place. A concept carries about five
    // descriptions, so this is one short sort per concept.
    byConcept
        .values()
        .forEach(descriptions -> descriptions.sort(DescriptionOrder.byLanguageTypeAndTerm()));
    return new DescriptionIndex(byConcept);
  }

  /**
   * Returns the descriptions of a concept.
   *
   * @param dense the dense identifier of the concept
   * @return the descriptions, ordered by language, then type, then term, empty if the concept has
   *     none
   */
  @Nonnull
  public List<Description> descriptionsOf(final int dense) {
    return byConcept.getOrDefault(dense, List.of());
  }
}
