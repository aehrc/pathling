package au.csiro.pathling.fhirpath.execution;

import static java.util.stream.Collectors.mapping;

import au.csiro.pathling.fhirpath.execution.DataRoot.JoinRoot;
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

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JoinSet {

  @Nonnull
  DataRoot master;

  @Nonnull
  List<JoinSet> children;


  @Nonnull
  public static JoinSet of(@Nonnull final DataRoot master, @Nonnull final List<JoinSet> children) {
    children.forEach(child -> {
      if (child.getMaster() instanceof JoinRoot jr) {
        if (!jr.getMaster().equals(master)) {
          throw new IllegalArgumentException(
              "Cannot have a join set when child master is differnt than set parent");
        }
      } else {
        throw new IllegalArgumentException("Child must be a join root");
      }
    });
    return new JoinSet(master, children);
  }

  @Nonnull
  public static JoinSet of(@Nonnull final DataRoot master) {
    return new JoinSet(master, List.of());
  }

  @Nonnull
  private static Stream<DataRoot> toStream(@Nonnull final DataRoot dataRoot) {
    if (dataRoot instanceof JoinRoot jr) {
      return Stream.concat(toStream(jr.getMaster()), Stream.of(dataRoot));
    }
    return Stream.of(dataRoot);
  }

  @Nonnull
  static List<DataRoot> toPath(@Nonnull final DataRoot root) {
    return toStream(root).toList();
  }

  @Nonnull
  private static List<JoinSet> mergeRoots(@Nonnull final List<List<DataRoot>> paths) {
    // convert to paths and then group by recurively by common prefixes
    // we have got it already somwhere else
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
   *
   * @param roots the roots to merge
   * @return the merged join sets
   */
  @Nonnull
  public static List<JoinSet> mergeRoots(@Nonnull final Set<DataRoot> roots) {
    // convert to paths and then group by recursively by common prefixes
    return mergeRoots(roots.stream().map(JoinSet::toPath).toList());
  }

  /**
   * Returns a string representation of the join set as a tree.
   */
  @Nonnull
  public String toTreeString() {
    return toTreeString(0);
  }

  /**
   * Prints a string representation of the join set as a tree.
   */
  public void printTree() {
    System.out.println(toTreeString());
  }

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
