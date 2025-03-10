package au.csiro.pathling.extract;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.extract.Tree.Leaf;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.ExternalConstantPath;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
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

  /**
   * Unnests a list of FhirPaths into a tree structure.
   *
   * @param paths The list of FhirPaths to unnest.
   * @return The tree structure representing the unnested paths.
   */
  @Nonnull
  public Tree<FhirPath> unnestPaths(@Nonnull final List<FhirPath> paths) {
    //
    return Tree.node(
        new ExternalConstantPath("%resource"),
        unnestPathsInternal(paths)
    );
  }

  /**
   * A function that converts a list of FhirPaths to the list of their corresponding expressions.
   *
   * @param paths The list of FhirPaths to convert.
   * @return The list of expressions.
   */
  @Nonnull
  private static List<String> asExpressions(@Nonnull final List<FhirPath> paths) {
    return paths.stream().map(FhirPath::toExpression).toList();
  }

  @Nonnull
  private static Tree<FhirPath> maybeUnnestingNode(
      @Nonnull final FhirPath prefix,
      @Nonnull final List<Tree<FhirPath>> children) {
    if (children.isEmpty()) {
      return Tree.Leaf.of(prefix);
    } else if (children.size() == 1 && !children.get(0).getValue().isNull()) {
      // TODO: reconsidre where to pefrom traversal optimisation
      //       for longr common traversal paths
      return children.get(0).mapValue(prefix::andThen);
    } else {
      return Tree.node(prefix, children);
    }
  }

  @Nonnull
  List<Tree<FhirPath>> unnestPathsInternal(@Nonnull final List<FhirPath> paths) {
    log.trace("Unnesting paths: {}", asExpressions(paths));
    if (paths.isEmpty()) {
      return Collections.emptyList();
    } else {
      // we need to split the paths into two groups:
      // - these that should be returned as is, which include $this and empty paths
      // - these that need to check for common prefixes with other paths

      final List<FhirPath> leafPaths = paths.stream()
          .filter(FhirPath::isNull)
          .toList();

      final List<Leaf<FhirPath>> leafNodes = leafPaths.stream().map(Leaf::of).toList();

      final List<FhirPath> unnestablePaths = paths.stream()
          .filter(Predicate.not(FhirPath::isNull))
          .toList();

      final Map<FhirPath, List<FhirPath>> groupedPaths = unnestablePaths.stream()
          .collect(
              groupingBy(FhirPath::head, LinkedHashMap::new, mapping(FhirPath::tail, toList())));
      final List<Tree<FhirPath>> unnestedNodes = groupedPaths.entrySet().stream()
          .flatMap(entry -> {
                // identify suffices that are aggregate functions and must not be unnested
                final List<FhirPath> aggSuffixes = entry.getValue().stream()
                    .filter(ImplicitUnnester::isAggregate)
                    .toList();
                // for each of the tree nodes append the current head to the path
                final List<Tree<FhirPath>> aggNodes = unnestPathsInternal(aggSuffixes)
                    .stream()
                    .map(tn -> tn.mapValue(v -> entry.getKey().andThen(v))).toList();
                // identify suffices that need to be unnested
                final List<FhirPath> suffixesToUnnest = entry.getValue().stream()
                    .filter(s -> !ImplicitUnnester.isAggregate(s))
                    .toList();
                // if needed wrap sub-trees in an unnesting node
                final Stream<Tree<FhirPath>> unnestedNodesStream =
                    suffixesToUnnest.isEmpty()
                    ? Stream.empty()
                    : Stream.of(maybeUnnestingNode(entry.getKey(),
                        unnestPathsInternal(suffixesToUnnest)));
                final List<Tree<FhirPath>> unnestNodes = unnestedNodesStream.toList();
                return Stream.concat(unnestNodes.stream(), aggNodes.stream());
              }
          ).toList();
      return Stream.concat(leafNodes.stream(), unnestedNodes.stream()).toList();
    }
  }

  private final static Set<String> AGG_FUNCTIONS = Set.of(
      "count", "sum", "first", "exists", "where");

  // Quite possibly all functions should be treated as aggregate functions
  static boolean isAggregate(@Nonnull final FhirPath path) {
    return (path.head() instanceof Paths.EvalFunction evalFunction)
        && AGG_FUNCTIONS.contains(evalFunction.getFunctionIdentifier());
  }

}
