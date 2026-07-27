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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TYPE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.RELATIONSHIP;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.roaringbitmap.RoaringBitmap;

/**
 * The attribute relationship index for one code system version. It records, per attribute type, the
 * target concepts of each source (forward navigation) and the source concepts of each target
 * (reverse navigation), so ECL attribute constraints and dotted attribute navigation resolve as
 * bitmap unions.
 *
 * @author John Grimes
 */
public final class RelationshipIndex {

  @Nonnull private final Map<String, Map<Integer, RoaringBitmap>> forward;
  @Nonnull private final Map<String, Map<Integer, RoaringBitmap>> reverse;

  private RelationshipIndex(
      @Nonnull final Map<String, Map<Integer, RoaringBitmap>> forward,
      @Nonnull final Map<String, Map<Integer, RoaringBitmap>> reverse) {
    this.forward = forward;
    this.reverse = reverse;
  }

  /**
   * Loads the relationship index for a code system version.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the loaded index
   */
  @Nonnull
  public static RelationshipIndex load(
      @Nonnull final TerminologyStoreReader reader, @Nonnull final String systemVersionId) {
    final Map<String, Map<Integer, RoaringBitmap>> forward = new HashMap<>();
    final Map<String, Map<Integer, RoaringBitmap>> reverse = new HashMap<>();
    reader.readTable(
        RELATIONSHIP,
        row -> {
          if (!systemVersionId.equals(row.getString(COLUMN_SYSTEM_VERSION_ID))) {
            return;
          }
          final String type = row.getString(COLUMN_TYPE_CODE);
          final int source = row.getInt(COLUMN_SOURCE_DENSE_ID);
          final int target = row.getInt(COLUMN_TARGET_DENSE_ID);
          forward
              .computeIfAbsent(type, k -> new HashMap<>())
              .computeIfAbsent(source, k -> new RoaringBitmap())
              .add(target);
          reverse
              .computeIfAbsent(type, k -> new HashMap<>())
              .computeIfAbsent(target, k -> new RoaringBitmap())
              .add(source);
        });
    return new RelationshipIndex(forward, reverse);
  }

  /**
   * Returns the attribute types present in this version.
   *
   * <p>The iteration order of the set is unspecified. A caller that turns these codes into a result
   * it hands back must impose its own order, because the order rows were read in is a property of
   * how the store happened to be written rather than of the code system.
   *
   * @return the set of attribute type codes
   */
  @Nonnull
  public Set<String> typeCodes() {
    return forward.keySet();
  }

  /**
   * Returns the concepts that are the value of a given attribute on any of the source concepts
   * (forward navigation).
   *
   * @param typeCode the attribute type code
   * @param sources the source concepts
   * @return the union of target concepts
   */
  @Nonnull
  public RoaringBitmap targetsOf(
      @Nonnull final String typeCode, @Nonnull final RoaringBitmap sources) {
    return gather(forward.get(typeCode), sources);
  }

  /**
   * Returns the concepts that have a given attribute pointing at any of the target concepts
   * (reverse navigation, the basis of attribute constraints).
   *
   * @param typeCode the attribute type code
   * @param targets the target concepts
   * @return the union of source concepts
   */
  @Nonnull
  public RoaringBitmap sourcesOf(
      @Nonnull final String typeCode, @Nonnull final RoaringBitmap targets) {
    return gather(reverse.get(typeCode), targets);
  }

  @Nonnull
  private static RoaringBitmap gather(
      final Map<Integer, RoaringBitmap> adjacency, @Nonnull final RoaringBitmap keys) {
    final RoaringBitmap result = new RoaringBitmap();
    if (adjacency == null) {
      return result;
    }
    keys.forEach(
        (org.roaringbitmap.IntConsumer)
            key -> {
              final RoaringBitmap values = adjacency.get(key);
              if (values != null) {
                result.or(values);
              }
            });
    return result;
  }
}
