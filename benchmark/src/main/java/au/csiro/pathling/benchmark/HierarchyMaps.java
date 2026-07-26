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

package au.csiro.pathling.benchmark;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CLOSURE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ANCESTOR_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DESCENDANT_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DIRECT;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * The four bitmap maps that make up a hierarchy index, held outside the production {@code
 * HierarchyIndex} so that the memory harness can permute and optimise them without changing any
 * production code.
 *
 * <p>The maps are built by exactly the construction the production index uses - a {@link HashMap}
 * with default capacity, filled by repeated {@link RoaringBitmap#add(int)} - because retained heap
 * depends on how a bitmap was grown as well as on what it contains. Building every variant the same
 * way is what makes the variants comparable. {@code HierarchyIndexMemoryHarnessTest} pins this
 * replica to the production index by asserting that the two answer every query identically over the
 * {@code rf2-mini} fixture.
 *
 * @author John Grimes
 */
public final class HierarchyMaps {

  /** The name of the map holding the transitive descendants of each concept. */
  public static final String DESCENDANTS = "descendants";

  /** The name of the map holding the transitive ancestors of each concept. */
  public static final String ANCESTORS = "ancestors";

  /** The name of the map holding the direct children of each concept. */
  public static final String CHILDREN = "children";

  /** The name of the map holding the direct parents of each concept. */
  public static final String PARENTS = "parents";

  @Nonnull private final Map<Integer, RoaringBitmap> descendants;
  @Nonnull private final Map<Integer, RoaringBitmap> ancestors;
  @Nonnull private final Map<Integer, RoaringBitmap> children;
  @Nonnull private final Map<Integer, RoaringBitmap> parents;

  private HierarchyMaps(
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
   * Loads the four maps for a code system version from a store's closure table.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the loaded maps
   */
  @Nonnull
  public static HierarchyMaps load(
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
    return new HierarchyMaps(descendants, ancestors, children, parents);
  }

  /**
   * Assembles the four maps directly, for tests that need a contrived hierarchy rather than a
   * store.
   *
   * @param descendants the transitive descendants of each concept
   * @param ancestors the transitive ancestors of each concept
   * @param children the direct children of each concept
   * @param parents the direct parents of each concept
   * @return the assembled maps
   */
  @Nonnull
  public static HierarchyMaps of(
      @Nonnull final Map<Integer, RoaringBitmap> descendants,
      @Nonnull final Map<Integer, RoaringBitmap> ancestors,
      @Nonnull final Map<Integer, RoaringBitmap> children,
      @Nonnull final Map<Integer, RoaringBitmap> parents) {
    return new HierarchyMaps(descendants, ancestors, children, parents);
  }

  /**
   * Returns the four maps in report order, keyed by name.
   *
   * @return an ordered, unmodifiable view of the four maps
   */
  @Nonnull
  public Map<String, Map<Integer, RoaringBitmap>> byName() {
    final Map<String, Map<Integer, RoaringBitmap>> named = new LinkedHashMap<>();
    named.put(DESCENDANTS, descendants);
    named.put(ANCESTORS, ancestors);
    named.put(CHILDREN, children);
    named.put(PARENTS, parents);
    return Collections.unmodifiableMap(named);
  }

  /**
   * Returns the direct children of each concept, which together with {@link #parents()} form the
   * is-a graph the depth-first pre-order traverses.
   *
   * @return the children map
   */
  @Nonnull
  public Map<Integer, RoaringBitmap> children() {
    return children;
  }

  /**
   * Returns the direct parents of each concept. A concept absent from this map has no parent and is
   * therefore a root of the is-a graph.
   *
   * @return the parents map
   */
  @Nonnull
  public Map<Integer, RoaringBitmap> parents() {
    return parents;
  }

  /**
   * Tests whether one concept subsumes another, by the same rule the production index applies: a
   * concept subsumes itself, and otherwise subsumption is membership of the ancestor's descendant
   * bitmap.
   *
   * @param ancestor the potential ancestor
   * @param descendant the potential descendant
   * @return true if {@code ancestor} subsumes {@code descendant}
   */
  public boolean subsumes(final int ancestor, final int descendant) {
    if (ancestor == descendant) {
      return true;
    }
    final RoaringBitmap bitmap = descendants.get(ancestor);
    return bitmap != null && bitmap.contains(descendant);
  }

  /**
   * Builds a new set of maps with every dense identifier, both key and member, replaced by its
   * image under a permutation. Applying the identity permutation therefore yields a structurally
   * identical copy, which is how the harness produces its baseline variant on the same footing as
   * the reordered ones.
   *
   * @param permutation maps each existing dense identifier to its replacement
   * @return the remapped maps
   */
  @Nonnull
  public HierarchyMaps remap(@Nonnull final int[] permutation) {
    return new HierarchyMaps(
        remapOne(descendants, permutation),
        remapOne(ancestors, permutation),
        remapOne(children, permutation),
        remapOne(parents, permutation));
  }

  /**
   * Asks every bitmap in all four maps to adopt run-length encoding wherever that is more space
   * efficient. The production index never does this, so no run container exists in it at any
   * ordering.
   *
   * @return true if the representation of at least one bitmap changed
   */
  public boolean runOptimize() {
    boolean changed = false;
    for (final Map<Integer, RoaringBitmap> map : byName().values()) {
      for (final RoaringBitmap bitmap : map.values()) {
        changed |= bitmap.runOptimize();
      }
    }
    return changed;
  }

  /**
   * Remaps one map. The source is traversed with {@link Map#forEach}, and never through a view such
   * as {@code entrySet()}, because a map caches the view object it hands out and a map carrying one
   * measures larger than an otherwise identical map that does not. Measuring the source is part of
   * the harness's job, so the harness must not leave a footprint on it.
   *
   * @param map the map to remap
   * @param permutation maps each existing dense identifier to its replacement
   * @return the remapped map
   */
  @Nonnull
  private static Map<Integer, RoaringBitmap> remapOne(
      @Nonnull final Map<Integer, RoaringBitmap> map, @Nonnull final int[] permutation) {
    final Map<Integer, RoaringBitmap> remapped = new HashMap<>();
    map.forEach(
        (key, bitmap) -> {
          final RoaringBitmap target =
              remapped.computeIfAbsent(permutation[key], k -> new RoaringBitmap());
          final IntIterator members = bitmap.getIntIterator();
          while (members.hasNext()) {
            target.add(permutation[members.next()]);
          }
        });
    return remapped;
  }
}
