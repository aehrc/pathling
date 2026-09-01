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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CLOSURE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ANCESTOR_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DIRECT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;

/**
 * The hierarchy index for one code system version, built from the precomputed transitive closure.
 * It answers descendant, ancestor, child, and parent queries as bitmap lookups, and subsumption as
 * a single bitmap membership test.
 *
 * @author John Grimes
 */
public final class HierarchyIndex {

  @Nonnull private final Map<Integer, RoaringBitmap> descendants;
  @Nonnull private final Map<Integer, RoaringBitmap> ancestors;
  @Nonnull private final Map<Integer, RoaringBitmap> children;
  @Nonnull private final Map<Integer, RoaringBitmap> parents;

  private HierarchyIndex(
      @Nonnull final Map<Integer, RoaringBitmap> descendants,
      @Nonnull final Map<Integer, RoaringBitmap> ancestors,
      @Nonnull final Map<Integer, RoaringBitmap> children,
      @Nonnull final Map<Integer, RoaringBitmap> parents) {
    this.descendants = descendants;
    this.ancestors = ancestors;
    this.children = children;
    this.parents = parents;
  }

  /**
   * Loads the hierarchy index for a code system version.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the loaded index
   */
  @Nonnull
  public static HierarchyIndex load(
      @Nonnull final TerminologyStoreReader reader, @Nonnull final String systemVersionId) {
    final Map<Integer, RoaringBitmap> descendants = new HashMap<>();
    final Map<Integer, RoaringBitmap> ancestors = new HashMap<>();
    final Map<Integer, RoaringBitmap> children = new HashMap<>();
    final Map<Integer, RoaringBitmap> parents = new HashMap<>();
    reader.readTable(
        CLOSURE,
        row -> {
          if (!systemVersionId.equals(row.getString(COLUMN_SYSTEM_VERSION_ID))) {
            return;
          }
          final int ancestor = row.getInt(COLUMN_ANCESTOR_DENSE_ID);
          final int descendant = row.getInt(COLUMN_DESCENDANT_DENSE_ID);
          descendants.computeIfAbsent(ancestor, k -> new RoaringBitmap()).add(descendant);
          ancestors.computeIfAbsent(descendant, k -> new RoaringBitmap()).add(ancestor);
          if (row.getBoolean(COLUMN_DIRECT)) {
            children.computeIfAbsent(ancestor, k -> new RoaringBitmap()).add(descendant);
            parents.computeIfAbsent(descendant, k -> new RoaringBitmap()).add(ancestor);
          }
        });
    // Building a bitmap by repeated addition never produces a run-length-encoded chunk, however
    // contiguous the values are; the representation has to be asked for. Over a full SNOMED CT UK
    // edition this saves a further 2.2% of the index's retained heap on top of what a hierarchy
    // ordering of dense identifiers achieves, and nothing where the values are scattered, since
    // run-length encoding is only chosen per chunk where it is the smaller representation.
    optimiseRepresentation(descendants, ancestors, children, parents);
    return new HierarchyIndex(descendants, ancestors, children, parents);
  }

  /**
   * Asks every bitmap to adopt run-length encoding wherever that is more space efficient. This
   * changes only how each set is represented, never its membership, so no query answer changes.
   *
   * @param maps the maps to optimise
   */
  @SafeVarargs
  private static void optimiseRepresentation(@Nonnull final Map<Integer, RoaringBitmap>... maps) {
    for (final Map<Integer, RoaringBitmap> map : maps) {
      map.values().forEach(RoaringBitmap::runOptimize);
    }
  }

  /**
   * Returns the strict descendants of a concept (excluding itself).
   *
   * @param dense the dense identifier
   * @return a copy of the descendants bitmap, empty if none
   */
  @Nonnull
  public RoaringBitmap descendantsOf(final int dense) {
    return copyOrEmpty(descendants.get(dense));
  }

  /**
   * Returns the strict ancestors of a concept (excluding itself).
   *
   * @param dense the dense identifier
   * @return a copy of the ancestors bitmap, empty if none
   */
  @Nonnull
  public RoaringBitmap ancestorsOf(final int dense) {
    return copyOrEmpty(ancestors.get(dense));
  }

  /**
   * Returns the direct children of a concept.
   *
   * @param dense the dense identifier
   * @return a copy of the children bitmap, empty if none
   */
  @Nonnull
  public RoaringBitmap childrenOf(final int dense) {
    return copyOrEmpty(children.get(dense));
  }

  /**
   * Returns the direct parents of a concept.
   *
   * @param dense the dense identifier
   * @return a copy of the parents bitmap, empty if none
   */
  @Nonnull
  public RoaringBitmap parentsOf(final int dense) {
    return copyOrEmpty(parents.get(dense));
  }

  /**
   * Tests whether one concept subsumes another (is an ancestor of it, or equal to it).
   *
   * @param ancestor the potential ancestor
   * @param descendant the potential descendant
   * @return true if {@code ancestor} subsumes {@code descendant}
   */
  public boolean subsumes(final int ancestor, final int descendant) {
    if (ancestor == descendant) {
      return true;
    }
    final RoaringBitmap d = descendants.get(ancestor);
    return d != null && d.contains(descendant);
  }

  @Nonnull
  private static RoaringBitmap copyOrEmpty(final RoaringBitmap bitmap) {
    return bitmap == null ? new RoaringBitmap() : bitmap.clone();
  }
}
