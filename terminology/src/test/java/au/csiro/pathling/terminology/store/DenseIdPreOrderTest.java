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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Verifies the depth-first pre-order used to assign dense identifiers: it is a bijection over the
 * whole concept dictionary, it is deterministic, and it terminates on a hierarchy that is a
 * directed acyclic graph rather than a tree.
 *
 * @author John Grimes
 */
class DenseIdPreOrderTest {

  @Test
  void assignsIdentifiersInPreOrderFromEachRoot() {
    // 0 is the root, with children 1 and 3; 1 has child 2; 3 has child 4. Edges are supplied out of
    // order to show the result does not depend on the order they arrive in.
    final int[] children = {2, 1, 4, 3};
    final int[] parents = {1, 0, 3, 0};

    final int[] permutation = DenseIdPreOrder.compute(children, parents, 5);

    // Visiting 0, then 1, then 1's child 2, then 3, then 3's child 4 leaves every identifier
    // where it already was, because this hierarchy is already in pre-order.
    assertArrayEquals(new int[] {0, 1, 2, 3, 4}, permutation);
  }

  @Test
  void placesEachSubtreeInAContiguousInterval() {
    // 0 is the root; 1 and 2 are its children; 1's children are 3 and 4. In code order the subtree
    // rooted at 1 is {1, 3, 4}, which is not contiguous. A pre-order makes it so.
    final int[] children = {1, 2, 3, 4};
    final int[] parents = {0, 0, 1, 1};

    final int[] permutation = DenseIdPreOrder.compute(children, parents, 5);

    assertArrayEquals(new int[] {0, 1, 4, 2, 3}, permutation);
    // The subtree rooted at 1 now occupies the interval [1, 3].
    assertEquals(1, permutation[1]);
    assertEquals(2, permutation[3]);
    assertEquals(3, permutation[4]);
  }

  @Test
  void visitsAConceptOnceWhenItIsReachableByMoreThanOnePath() {
    // 3 is a child of both 1 and 2, so the hierarchy is a directed acyclic graph, not a tree.
    final int[] children = {1, 2, 3, 3};
    final int[] parents = {0, 0, 1, 2};

    final int[] permutation = DenseIdPreOrder.compute(children, parents, 4);

    assertArrayEquals(new int[] {0, 1, 3, 2}, permutation);
  }

  @Test
  void assignsAnIdentifierToEveryConceptOutsideTheHierarchy() {
    // Only 0 and 1 take part in the hierarchy. Concepts 2, 3 and 4 have no is-a edge at all,
    // which is the case for inactive concepts and for referenced-only metadata concepts.
    final int[] children = {1};
    final int[] parents = {0};

    final int[] permutation = DenseIdPreOrder.compute(children, parents, 5);

    assertBijection(permutation, 5);
    // Concepts outside the hierarchy take the highest identifiers and keep their existing order.
    assertArrayEquals(new int[] {0, 1, 2, 3, 4}, permutation);
  }

  @Test
  void isABijectionWhenTheHierarchyHasSeveralRoots() {
    // Two disjoint trees plus an isolated concept. Roots are visited in ascending order, so
    // the tree rooted at 1 comes before the tree rooted at 4.
    final int[] children = {2, 3, 5};
    final int[] parents = {1, 1, 4};

    final int[] permutation = DenseIdPreOrder.compute(children, parents, 7);

    assertBijection(permutation, 7);
    assertEquals(0, permutation[1]);
    assertEquals(1, permutation[2]);
    assertEquals(2, permutation[3]);
    assertEquals(3, permutation[4]);
    assertEquals(4, permutation[5]);
    // 0 and 6 have no is-a edge, so they are appended in their existing order.
    assertEquals(5, permutation[0]);
    assertEquals(6, permutation[6]);
  }

  @Test
  void isDeterministicAcrossRepeatedComputations() {
    final int[] children = {3, 1, 4, 2, 5};
    final int[] parents = {1, 0, 1, 0, 2};

    final int[] first = DenseIdPreOrder.compute(children, parents, 8);
    final int[] second = DenseIdPreOrder.compute(children, parents, 8);

    assertArrayEquals(first, second);
  }

  @Test
  void terminatesOnAHierarchyThatContainsACycle() {
    // A cycle should never occur in a real release, but a malformed one must not hang the import. A
    // cyclic component has no root, so it is never entered by the traversal and its members are
    // appended with the other unreached concepts.
    final int[] children = {1, 2, 0};
    final int[] parents = {0, 1, 2};

    final int[] permutation = DenseIdPreOrder.compute(children, parents, 3);

    assertBijection(permutation, 3);
  }

  @Test
  void handlesAnEmptyHierarchy() {
    final int[] permutation = DenseIdPreOrder.compute(new int[0], new int[0], 3);

    assertArrayEquals(new int[] {0, 1, 2}, permutation);
  }

  @Test
  void rejectsMismatchedEdgeArrays() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DenseIdPreOrder.compute(new int[] {1, 2}, new int[] {0}, 3));
  }

  @Test
  void rejectsAnEdgeOutsideTheDictionary() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DenseIdPreOrder.compute(new int[] {5}, new int[] {0}, 3));
  }

  /** Asserts that a permutation assigns each identifier in the range exactly once. */
  private static void assertBijection(final int[] permutation, final int conceptCount) {
    assertEquals(conceptCount, permutation.length);
    final boolean[] seen = new boolean[conceptCount];
    for (final int assigned : permutation) {
      assertTrue(assigned >= 0 && assigned < conceptCount, "Assigned identifier out of range");
      assertFalse(seen[assigned], "Identifier " + assigned + " was assigned twice");
      seen[assigned] = true;
    }
  }
}
