package au.csiro.pathling.extract;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.extract.Tree.Leaf;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.ExternalConstantPath;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A class that unnests a list of FhirPaths into a tree structure.
 * <p>
 * Each non-leaf node in tree represents a common prefix of the paths (which should be evaluated
 * using the forEachOnNull clause  SQL on FHIR views).
 * <p>
 * Each leaf node in the tree represents a path that should be evaluated as c column selection (and
 * in all cases it should be $this)
 * <p>
 * The unnested createes the "%resource" node as the root of the tree. The unnester explicitly
 * unnests (flattens) leaf nodes paths that so they represent singular expressions. For example the
 * paths:
 * <pre>
 *   $name.given
 *   $name.family
 *
 * </pre>
 * producs the following tree:
 * <pre>
 *   %resource
 *     name
 *      given
 *        $this
 *      family
 *        $this
 * </pre>
 */
@Slf4j
public class ImplicitUnnester {

  @Value(staticConstructor = "of")
  public static class FhirPathWithTag {

    @Nonnull
    FhirPath path;
    @Nullable
    String tag;

    public static FhirPathWithTag of(@Nonnull final FhirPath path) {
      return new FhirPathWithTag(path, null);
    }

    @Nonnull
    public String toExpression() {
      return path.toExpression();
    }

    public boolean isNull() {
      return path.isNull();
    }

    @Nonnull
    public FhirPath prefix() {
      return path.prefix();
    }

    @Nonnull
    public FhirPathWithTag suffix() {
      return FhirPathWithTag.of(path.suffix(), tag);
    }

    @Nonnull
    public FhirPathWithTag withPrefix(@Nonnull final FhirPath prefix) {
      return FhirPathWithTag.of(prefix.andThen(path), tag);
    }

    @Nonnull
    public String getRequiredTag() {
      return requireNonNull(tag);
    }
  }


  /**
   * Unnests a list of FhirPaths into a tree structure.
   *
   * @param paths The list of FhirPaths to unnest.
   * @return The tree structure representing the unnested paths.
   */
  @Nonnull
  public Tree<FhirPathWithTag> unnestPaths(@Nonnull final List<FhirPathWithTag> paths) {
    //
    return Tree.node(
        FhirPathWithTag.of(new ExternalConstantPath("%resource")), unnestPathsInternal(paths)
    );
  }

  /**
   * A function that converts a list of FhirPaths to the list of their corresponding expressions.
   *
   * @param paths The list of FhirPaths to convert.
   * @return The list of expressions.
   */
  @Nonnull
  private static List<String> asExpressions(@Nonnull final List<FhirPathWithTag> paths) {
    return paths.stream().map(FhirPathWithTag::toExpression).toList();
  }

  @Nonnull
  private static Tree<FhirPathWithTag> maybeUnnestingNode(
      @Nonnull final FhirPath prefix,
      @Nonnull final List<Tree<FhirPathWithTag>> children) {
    if (children.isEmpty()) {
      throw new IllegalStateException("Empty children list passed to maybeUnnestingNode");
    } else if (children.size() == 1 && !children.get(0).getValue().isNull()) {
      return children.get(0).mapValue(v -> v.withPrefix(prefix));
    } else {
      return Tree.node(FhirPathWithTag.of(prefix), children);
    }
  }

  @Nonnull
  List<Tree<FhirPathWithTag>> unnestPathsInternal(@Nonnull final List<FhirPathWithTag> paths) {
    log.trace("Unnesting paths: {}", asExpressions(paths));
    if (paths.isEmpty()) {
      return Collections.emptyList();
    } else {
      // we need to split the paths into two groups:
      // - these that should be returned as is, which include $this and empty paths
      // - these that need to check for common prefixes with other paths

      final List<FhirPathWithTag> leafPaths = paths.stream()
          .filter(FhirPathWithTag::isNull)
          .toList();

      final List<Leaf<FhirPathWithTag>> leafNodes = leafPaths.stream().map(Leaf::of).toList();

      final List<FhirPathWithTag> unnestablePaths = paths.stream()
          .filter(Predicate.not(FhirPathWithTag::isNull))
          .toList();

      final Map<FhirPath, List<FhirPathWithTag>> groupedPaths = unnestablePaths.stream()
          .collect(
              groupingBy(FhirPathWithTag::prefix, LinkedHashMap::new,
                  mapping(FhirPathWithTag::suffix, toList())));
      final List<Tree<FhirPathWithTag>> unnestedNodes = groupedPaths.entrySet().stream()
          .flatMap(entry -> {
                // identify suffices that are  functions and must not be unnested
                final List<FhirPathWithTag> funcSuffixes = entry.getValue().stream()
                    .filter(ImplicitUnnester::isFunction)
                    .toList();
                // for each of the tree nodes append the current head to the path
                final List<Tree<FhirPathWithTag>> funcNodes = unnestPathsInternal(funcSuffixes)
                    .stream()
                    .map(tn -> tn.mapValue(v -> v.withPrefix(entry.getKey())))
                    .toList();
                // identify suffices that need to be unnested
                final List<FhirPathWithTag> suffixesToUnnest = entry.getValue().stream()
                    .filter(Predicate.not(ImplicitUnnester::isFunction))
                    .toList();
                // if needed wrap sub-trees in an unnesting node
                final Stream<Tree<FhirPathWithTag>> unnestedNodesStream =
                    suffixesToUnnest.isEmpty()
                    ? Stream.empty()
                    : Stream.of(maybeUnnestingNode(entry.getKey(),
                        unnestPathsInternal(suffixesToUnnest)));
                final List<Tree<FhirPathWithTag>> unnestNodes = unnestedNodesStream.toList();
                return Stream.concat(unnestNodes.stream(), funcNodes.stream());
              }
          ).toList();
      return Stream.concat(leafNodes.stream(), unnestedNodes.stream()).toList();
    }
  }

  // Quite possibly all functions should be treated as aggregate functions
  static boolean isFunction(@Nonnull final FhirPathWithTag path) {
    return (path.getPath().head() instanceof Paths.EvalFunction);
  }
}
