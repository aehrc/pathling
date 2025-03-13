/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.execution;

import static java.util.stream.Collectors.mapping;

import au.csiro.pathling.fhirpath.execution.DataRoot.JoinRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Represents a set of joins in a hierarchical structure.
 * This class is used to model the relationships between different data roots
 * in a FHIR path execution context.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JoinSet {

  /**
   * The master data root of this join set.
   */
  @Nonnull
  DataRoot master;

  /**
   * The children of this join set.
   * Each child is a join set that has a join root with this join set's master as its master.
   */
  @Nonnull
  List<JoinSet> children;

  /**
   * Gets the master resource root of this join set.
   * This is a convenience method that casts the master to a resource root.
   *
   * @return The master resource root
   * @throws ClassCastException if the master is not a resource root
   */
  @Nonnull
  public ResourceRoot getMasterResourceRoot() {
    return (ResourceRoot) master;
  }

  /**
   * Creates a new join set with the given master and children.
   * This method validates that each child has a join root with the given master as its master.
   *
   * @param master The master data root
   * @param children The children of the join set
   * @return A new join set
   * @throws IllegalArgumentException if any child does not have a join root with the given master as its master
   */
  @Nonnull
  public static JoinSet of(@Nonnull final DataRoot master, @Nonnull final List<JoinSet> children) {
    children.forEach(child -> {
      if (child.getMaster() instanceof JoinRoot jr) {
        if (!jr.getMaster().equals(master)) {
          throw new IllegalArgumentException(
              "Cannot have a join set when child master is different than set master");
        }
      } else {
        throw new IllegalArgumentException("Child must be a join root");
      }
    });
    return new JoinSet(master, children);
  }

  /**
   * Creates a new join set with the given master and no children.
   *
   * @param master The master data root
   * @return A new join set
   */
  @Nonnull
  public static JoinSet of(@Nonnull final DataRoot master) {
    return new JoinSet(master, List.of());
  }

  /**
   * Converts a data root to a stream of data roots.
   * If the data root is a join root, this method recursively includes its master.
   *
   * @param dataRoot The data root to convert
   * @return A stream of data roots
   */
  @Nonnull
  private static Stream<DataRoot> toStream(@Nonnull final DataRoot dataRoot) {
    if (dataRoot instanceof JoinRoot jr) {
      return Stream.concat(toStream(jr.getMaster()), Stream.of(dataRoot));
    }
    return Stream.of(dataRoot);
  }

  /**
   * Converts a data root to a path of data roots.
   * This is a convenience method that collects the stream from {@link #toStream(DataRoot)} to a list.
   *
   * @param root The data root to convert
   * @return A path of data roots
   */
  @Nonnull
  static List<DataRoot> toPath(@Nonnull final DataRoot root) {
    return toStream(root).toList();
  }

  /**
   * Merges a list of paths into a tree of join sets by recursively grouping by common prefixes.
   *
   * @param paths The paths to merge
   * @return The merged join sets
   */
  @Nonnull
  private static List<JoinSet> mergeRoots(@Nonnull final List<List<DataRoot>> paths) {
    // Convert to paths and then group recursively by common prefixes
    final Map<DataRoot, List<List<DataRoot>>> suffixesByHeads = paths.stream()
        .filter(Predicate.not(List::isEmpty))
        .collect(Collectors.groupingBy(
            path -> path.get(0),
            mapping(path -> path.subList(1, path.size()), Collectors.toList())
        ));

    return suffixesByHeads.entrySet().stream()
        .map(entry -> JoinSet.of(entry.getKey(), mergeRoots(entry.getValue())))
        .toList();
  }

  /**
   * Merges a set of roots into a tree of join sets by recursively grouping by common prefixes.
   * This method converts each root to a path and then merges the paths.
   *
   * @param roots The roots to merge
   * @return The merged join sets
   */
  @Nonnull
  public static List<JoinSet> mergeRoots(@Nonnull final Set<DataRoot> roots) {
    // Convert to paths and then group by recursively by common prefixes
    return mergeRoots(roots.stream().map(JoinSet::toPath).toList());
  }

  /**
   * Returns a string representation of the join set as a tree.
   * This is useful for debugging and logging.
   *
   * @return A string representation of the join set as a tree
   */
  @Nonnull
  public String toTreeString() {
    return toTreeString(0);
  }

  /**
   * Prints a string representation of the join set as a tree to the standard output.
   * This is useful for debugging.
   */
  //noinspection unused
  public void printTree() {
    System.out.println(toTreeString());
  }

  /**
   * Returns a string representation of the join set as a tree with the given depth.
   * This is a helper method for {@link #toTreeString()}.
   *
   * @param depth The depth of the join set in the tree
   * @return A string representation of the join set as a tree
   */
  @Nonnull
  private String toTreeString(final int depth) {
    final StringBuilder sb = new StringBuilder();
    sb.append("  ".repeat(depth > 0
                          ? depth - 1
                          : 0));
    sb.append(depth > 0
              ? "+-"
              : "");
    sb.append(master.toDisplayString());
    sb.append("\n");
    children.forEach(child -> sb.append(child.toTreeString(depth + 1)));
    return sb.toString();
  }


}
