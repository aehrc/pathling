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
import java.util.Arrays;

/**
 * Computes a depth-first pre-order over a concept hierarchy, expressed as a permutation of dense
 * identifiers. Placing each subtree in a near-contiguous interval lets the runtime hierarchy index
 * represent a descendant set as a few compressed chunks instead of many scattered ones.
 *
 * <p>The traversal runs on the driver over primitive arrays. A depth-first order is inherently
 * sequential, so distributing it would buy nothing but shuffles, and the graph is small in absolute
 * terms: the largest edition on hand has around 1.1 million concepts and 1.5 million active is-a
 * edges, which is a few tens of megabytes against the several hundred the index itself occupies.
 * The traversal is iterative rather than recursive, because a real hierarchy is deep enough to make
 * recursion a stack risk.
 *
 * @author John Grimes
 */
public final class DenseIdPreOrder {

  /** Marks a concept the traversal has not yet reached. */
  private static final int UNASSIGNED = -1;

  private DenseIdPreOrder() {
    // Utility class.
  }

  /**
   * Computes the pre-order permutation for a hierarchy.
   *
   * <p>Roots - concepts with no parent - are visited in ascending identifier order, and each
   * concept's children likewise, so the result depends only on the hierarchy and not on the order
   * the edges arrive in. Repeated imports of the same release therefore assign identical
   * identifiers.
   *
   * <p>Every concept the traversal does not reach keeps its existing relative order and is appended
   * after it. That covers inactive concepts, metadata concepts that are only ever referenced, and -
   * should a malformed release contain one - a cyclic component, which has no root and so is never
   * entered. Dense identifiers address the whole concept dictionary rather than only the hierarchy,
   * so a permutation that omitted any concept would not be a bijection.
   *
   * @param children the child of each is-a edge, by existing dense identifier
   * @param parents the parent of each is-a edge, by existing dense identifier, parallel to {@code
   *     children}
   * @param conceptCount the number of concepts in the dictionary
   * @return a permutation of {@code [0, conceptCount)}, giving each concept's new identifier
   * @throws IllegalArgumentException if the two edge arrays differ in length, or if an edge refers
   *     to a concept outside the dictionary
   */
  @Nonnull
  public static int[] compute(
      @Nonnull final int[] children, @Nonnull final int[] parents, final int conceptCount) {
    if (children.length != parents.length) {
      throw new IllegalArgumentException(
          "The is-a edge arrays must be the same length, but were "
              + children.length
              + " and "
              + parents.length);
    }
    if (conceptCount < 0) {
      throw new IllegalArgumentException("The concept count cannot be negative: " + conceptCount);
    }

    final boolean[] hasParent = new boolean[conceptCount];
    final int[] childCounts = new int[conceptCount];
    for (int edge = 0; edge < children.length; edge++) {
      checkWithinDictionary(children[edge], conceptCount);
      checkWithinDictionary(parents[edge], conceptCount);
      hasParent[children[edge]] = true;
      childCounts[parents[edge]]++;
    }

    // Adjacency in compressed form: childrenOf[start[n]] up to childrenOf[start[n + 1]] are the
    // children of concept n. One pair of arrays for the whole graph keeps the traversal free of the
    // per-concept collections a map of lists would allocate.
    final int[] start = new int[conceptCount + 1];
    for (int node = 0; node < conceptCount; node++) {
      start[node + 1] = start[node] + childCounts[node];
    }
    final int[] childrenOf = new int[children.length];
    final int[] cursor = Arrays.copyOf(start, conceptCount);
    for (int edge = 0; edge < children.length; edge++) {
      childrenOf[cursor[parents[edge]]++] = children[edge];
    }
    for (int node = 0; node < conceptCount; node++) {
      Arrays.sort(childrenOf, start[node], start[node + 1]);
    }

    final int[] permutation = new int[conceptCount];
    Arrays.fill(permutation, UNASSIGNED);
    int next = 0;

    // Each concept is pushed at most once per incoming edge, and each root once, which
    // bounds the stack.
    final int[] stack = new int[conceptCount + children.length];
    int depth = 0;
    // Push the roots in descending order, so that the smallest is popped, and visited, first.
    for (int node = conceptCount - 1; node >= 0; node--) {
      if (!hasParent[node] && start[node] < start[node + 1]) {
        stack[depth++] = node;
      }
    }
    while (depth > 0) {
      final int node = stack[--depth];
      if (permutation[node] != UNASSIGNED) {
        continue;
      }
      permutation[node] = next++;
      for (int index = start[node + 1] - 1; index >= start[node]; index--) {
        if (permutation[childrenOf[index]] == UNASSIGNED) {
          stack[depth++] = childrenOf[index];
        }
      }
    }

    for (int node = 0; node < conceptCount; node++) {
      if (permutation[node] == UNASSIGNED) {
        permutation[node] = next++;
      }
    }
    return permutation;
  }

  private static void checkWithinDictionary(final int dense, final int conceptCount) {
    if (dense < 0 || dense >= conceptCount) {
      throw new IllegalArgumentException(
          "An is-a edge refers to dense identifier "
              + dense
              + ", which is outside a dictionary of "
              + conceptCount
              + " concepts");
    }
  }
}
