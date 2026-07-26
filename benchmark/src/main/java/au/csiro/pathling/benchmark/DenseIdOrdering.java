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

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Rules for assigning dense identifiers to concepts. A rule is expressed as a permutation, an array
 * whose value at each existing dense identifier is that concept's replacement identifier.
 *
 * <p>Two rules are compared by the memory harness: the identity, which reproduces the assignment
 * the importer makes today (concept code order), and a depth-first pre-order over the is-a graph,
 * which places each subtree in a near-contiguous interval so that a descendant set can compress
 * into a few runs rather than many scattered chunks.
 *
 * @author John Grimes
 */
public final class DenseIdOrdering {

  private DenseIdOrdering() {
    // Utility class.
  }

  /**
   * Returns the permutation that leaves every identifier where it is, which is the assignment the
   * importer makes today.
   *
   * @param conceptCount the number of concepts in the dictionary
   * @return the identity permutation
   */
  @Nonnull
  public static int[] identity(final int conceptCount) {
    final int[] permutation = new int[conceptCount];
    for (int dense = 0; dense < conceptCount; dense++) {
      permutation[dense] = dense;
    }
    return permutation;
  }

  /**
   * Computes a depth-first pre-order over the is-a graph held in a hierarchy index's direct-edge
   * maps, and returns it as a permutation of dense identifiers.
   *
   * <p>Roots - concepts with no parent - are visited in ascending identifier order, and each
   * concept's children likewise, so the result depends only on the graph and not on map iteration
   * order. The traversal is iterative, because a real hierarchy is deep enough to make recursion a
   * stack risk, and it marks concepts as it assigns them, so a concept reachable by more than one
   * path is visited once and the traversal terminates on a directed acyclic graph.
   *
   * <p>Concepts the traversal never reaches - inactive concepts, and any concept with no is-a edge
   * - keep their relative order and are appended after it. Dense identifiers address the whole
   * concept dictionary rather than only the hierarchy, so a permutation that omitted them would not
   * be a bijection and would corrupt every other index.
   *
   * @param maps the hierarchy index maps supplying the is-a graph
   * @param conceptCount the number of concepts in the dictionary
   * @return a permutation of {@code [0, conceptCount)} in depth-first pre-order
   * @throws IllegalArgumentException if the graph refers to a concept outside the dictionary
   */
  @Nonnull
  public static int[] preOrder(@Nonnull final HierarchyMaps maps, final int conceptCount) {
    final Map<Integer, RoaringBitmap> children = maps.children();
    final Map<Integer, RoaringBitmap> parents = maps.parents();
    checkWithinDictionary(children, conceptCount);
    checkWithinDictionary(parents, conceptCount);

    // A concept that has children but no parent is a root of the is-a graph.
    final List<Integer> roots = new ArrayList<>();
    for (final Integer node : children.keySet()) {
      if (!parents.containsKey(node)) {
        roots.add(node);
      }
    }
    Collections.sort(roots);

    final int[] permutation = new int[conceptCount];
    Arrays.fill(permutation, -1);
    int next = 0;

    // A concept with several parents can be pushed once per parent before it is first
    // popped, so the stack is sized by the number of edges, not the number of concepts.
    final int[] stack = new int[roots.size() + edgeCount(children)];
    int depth = 0;
    // Push the roots in descending order so the smallest is popped, and visited, first.
    for (int index = roots.size() - 1; index >= 0; index--) {
      stack[depth++] = roots.get(index);
    }
    while (depth > 0) {
      final int node = stack[--depth];
      if (permutation[node] != -1) {
        continue;
      }
      permutation[node] = next++;
      depth = pushChildren(children.get(node), stack, depth, permutation);
    }

    // Every concept the traversal did not reach keeps its existing relative order.
    for (int dense = 0; dense < conceptCount; dense++) {
      if (permutation[dense] == -1) {
        permutation[dense] = next++;
      }
    }
    return permutation;
  }

  /**
   * Counts the edges in a direct-edge map, which bounds how many entries the traversal stack can
   * hold at once.
   *
   * @param edges a map from concept to its directly related concepts
   * @return the total number of edges
   */
  private static int edgeCount(@Nonnull final Map<Integer, RoaringBitmap> edges) {
    long total = 0;
    for (final RoaringBitmap targets : edges.values()) {
      total += targets.getLongCardinality();
    }
    if (total > Integer.MAX_VALUE - 8) {
      throw new IllegalArgumentException("Hierarchy has too many edges to traverse: " + total);
    }
    return (int) total;
  }

  /**
   * Pushes a concept's not-yet-assigned children onto the stack in descending order, so that they
   * are popped in ascending order.
   *
   * @param children the concept's children, or null if it has none
   * @param stack the traversal stack
   * @param depth the current stack depth
   * @param permutation the assignment so far, used to skip concepts already visited
   * @return the new stack depth
   */
  private static int pushChildren(
      final RoaringBitmap children,
      @Nonnull final int[] stack,
      final int depth,
      @Nonnull final int[] permutation) {
    if (children == null) {
      return depth;
    }
    // The bitmap iterates in ascending order, so collect first and push in reverse.
    final int[] pending = new int[children.getCardinality()];
    int count = 0;
    final IntIterator iterator = children.getIntIterator();
    while (iterator.hasNext()) {
      final int child = iterator.next();
      if (permutation[child] == -1) {
        pending[count++] = child;
      }
    }
    int newDepth = depth;
    for (int index = count - 1; index >= 0; index--) {
      stack[newDepth++] = pending[index];
    }
    return newDepth;
  }

  /**
   * Checks that every identifier the graph mentions addresses a concept in the dictionary, so that
   * the permutation it produces can be a bijection. The map is traversed with {@link Map#forEach}
   * rather than through a view, because a map caches the view object it hands out and would then
   * measure larger than one that was never traversed.
   *
   * @param map a map from concept to its directly related concepts
   * @param conceptCount the number of concepts in the dictionary
   * @throws IllegalArgumentException if the graph refers to a concept outside the dictionary
   */
  private static void checkWithinDictionary(
      @Nonnull final Map<Integer, RoaringBitmap> map, final int conceptCount) {
    map.forEach(
        (key, targets) -> {
          if (key < 0 || key >= conceptCount) {
            throw new IllegalArgumentException(
                "Hierarchy refers to dense identifier "
                    + key
                    + ", which is outside a dictionary of "
                    + conceptCount
                    + " concepts");
          }
          if (!targets.isEmpty() && (targets.first() < 0 || targets.last() >= conceptCount)) {
            throw new IllegalArgumentException(
                "Hierarchy relates concept "
                    + key
                    + " to a dense identifier outside a dictionary of "
                    + conceptCount
                    + " concepts");
          }
        });
  }
}
