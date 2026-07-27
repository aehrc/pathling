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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFERENCED_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_REFSET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.REFSET_MEMBER;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;

/**
 * The reference set membership index for one code system version: which concepts belong to each
 * reference set, and the association targets that drive SNOMED implicit concept maps.
 *
 * @author John Grimes
 */
public final class RefsetIndex {

  @Nonnull private final Map<String, RoaringBitmap> members;
  @Nonnull private final Map<String, Map<Integer, String>> associationTargets;

  private RefsetIndex(
      @Nonnull final Map<String, RoaringBitmap> members,
      @Nonnull final Map<String, Map<Integer, String>> associationTargets) {
    this.members = members;
    this.associationTargets = associationTargets;
  }

  /**
   * Loads the reference set index for a code system version.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the loaded index
   */
  @Nonnull
  public static RefsetIndex load(
      @Nonnull final TerminologyStoreReader reader, @Nonnull final String systemVersionId) {
    final Map<String, RoaringBitmap> members = new HashMap<>();
    final Map<String, Map<Integer, String>> associationTargets = new HashMap<>();
    reader.readTable(
        REFSET_MEMBER,
        row -> {
          if (!systemVersionId.equals(row.getString(COLUMN_SYSTEM_VERSION_ID))) {
            return;
          }
          final String refset = row.getString(COLUMN_REFSET_CODE);
          final int referenced = row.getInt(COLUMN_REFERENCED_DENSE_ID);
          members.computeIfAbsent(refset, k -> new RoaringBitmap()).add(referenced);
          final String target = row.getString(COLUMN_TARGET_CODE);
          if (target != null) {
            associationTargets
                .computeIfAbsent(refset, k -> new HashMap<>())
                .put(referenced, target);
          }
        });
    return new RefsetIndex(members, associationTargets);
  }

  /**
   * Returns the members of a reference set.
   *
   * @param refsetCode the reference set identifier
   * @return a copy of the members bitmap, empty if the reference set is unknown
   */
  @Nonnull
  public RoaringBitmap membersOf(@Nonnull final String refsetCode) {
    final RoaringBitmap bitmap = members.get(refsetCode);
    return bitmap == null ? new RoaringBitmap() : bitmap.clone();
  }

  /**
   * Returns the association targets of a reference set as a map from referenced concept dense
   * identifier to the target code.
   *
   * <p>The iteration order of the map is unspecified. A caller that turns these entries into a
   * result it hands back must impose its own order, because the order rows were read in is a
   * property of how the store happened to be written rather than of the reference set.
   *
   * @param refsetCode the reference set identifier
   * @return the association target map, empty if the reference set has no targets
   */
  @Nonnull
  public Map<Integer, String> associationTargets(@Nonnull final String refsetCode) {
    return associationTargets.getOrDefault(refsetCode, Map.of());
  }
}
