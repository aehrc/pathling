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
 * A class that unnests a list of FhirPaths into a hierarchical tree structure to ensure valid data combinations.
 * <p>
 * The purpose of this class is to transform a flat list of FHIRPath expressions into a tree structure
 * that groups columns by their common prefixes. This grouping ensures that when extracting data,
 * only valid combinations of values are included in the result, preserving the relationships between
 * nested elements in the source data.
 * <p>
 * For example, when extracting both given names and family names from Patient resources, this structure
 * ensures that each row in the result contains only name combinations that actually exist in the data,
 * rather than a Cartesian product of all possible combinations.
 * <p>
 * In the resulting tree structure:
 * <ul>
 *   <li>Each non-leaf node represents a common prefix shared by multiple paths (which should be evaluated
 *       using the forEachOnNull clause in SQL on FHIR views)</li>
 *   <li>Each leaf node represents a path that should be evaluated as a column selection (and
 *       in all cases it should be $this)</li>
 * </ul>
 * <p>
 * The unnester creates the "%resource" node as the root of the tree. The unnester explicitly
 * unnests (flattens) leaf node paths so they represent singular expressions. For example the
 * paths:
 * <pre>
 *   $name.given
 *   $name.family
 * </pre>
 * produce the following tree:
 * <pre>
 *   %resource
 *     name
 *       given
 *         $this
 *       family
 *         $this
 * </pre>
 * <p>
 * Special handling is applied to function calls and operators to ensure they are processed correctly
 * within the tree structure.
 */
@Slf4j
public class ImplicitUnnester {

  /**
   * A wrapper class for FhirPath objects that can include an optional tag.
   * <p>
   * This class provides methods to manipulate FhirPath objects while preserving
   * any associated tags. It delegates most operations to the underlying FhirPath
   * while maintaining the tag context.
   */
  @Value(staticConstructor = "of")
  public static class FhirPathWithTag {

    /**
     * The underlying FhirPath object.
     */
    @Nonnull
    FhirPath path;
    
    /**
     * An optional tag that can be associated with the path.
     * This can be used to store metadata about the path.
     */
    @Nullable
    String tag;

    /**
     * Creates a new FhirPathWithTag with the given path and no tag.
     *
     * @param path The FhirPath to wrap
     * @return A new FhirPathWithTag instance
     */
    public static FhirPathWithTag of(@Nonnull final FhirPath path) {
      return new FhirPathWithTag(path, null);
    }

    /**
     * Returns the string representation of the underlying path.
     *
     * @return The path expression as a string
     */
    @Nonnull
    public String toExpression() {
      return path.toExpression();
    }

    /**
     * Checks if the underlying path is a null path.
     *
     * @return true if the path is null, false otherwise
     */
    public boolean isNull() {
      return path.isNull();
    }

    /**
     * Gets the prefix of the underlying path.
     *
     * @return The prefix of the path
     */
    @Nonnull
    public FhirPath prefix() {
      return path.prefix();
    }

    /**
     * Gets the suffix of the underlying path, preserving the tag.
     *
     * @return A new FhirPathWithTag containing the suffix and the original tag
     */
    @Nonnull
    public FhirPathWithTag suffix() {
      return FhirPathWithTag.of(path.suffix(), tag);
    }

    /**
     * Creates a new FhirPathWithTag by prepending the given prefix to the current path.
     *
     * @param prefix The prefix to prepend
     * @return A new FhirPathWithTag with the combined path and the original tag
     */
    @Nonnull
    public FhirPathWithTag withPrefix(@Nonnull final FhirPath prefix) {
      return FhirPathWithTag.of(prefix.andThen(path), tag);
    }

    /**
     * Gets the tag, throwing an exception if it's null.
     *
     * @return The non-null tag
     * @throws NullPointerException if the tag is null
     */
    @Nonnull
    public String getRequiredTag() {
      return requireNonNull(tag);
    }
  }


  /**
   * Unnests a list of FhirPaths into a tree structure.
   * <p>
   * This is the main entry point for the unnesting process. It creates a root node
   * with the special "%resource" identifier and then delegates to the internal
   * unnesting method to process the paths.
   * <p>
   * The resulting tree structure preserves the relationships between nested elements,
   * ensuring that only valid combinations of values are included when the data is extracted.
   *
   * @param paths The list of FhirPaths to unnest
   * @return A tree structure representing the unnested paths with "%resource" as the root
   */
  @Nonnull
  public Tree<FhirPathWithTag> unnestPaths(@Nonnull final List<FhirPathWithTag> paths) {
    // Create a tree with "%resource" as the root node and the unnested paths as children
    return Tree.node(
        FhirPathWithTag.of(new ExternalConstantPath("%resource")), unnestPathsInternal(paths)
    );
  }

  /**
   * Converts a list of FhirPaths to a list of their corresponding string expressions.
   * <p>
   * This is primarily used for logging and debugging purposes to provide a human-readable
   * representation of the paths being processed.
   *
   * @param paths The list of FhirPaths to convert
   * @return A list of string expressions representing the paths
   */
  @Nonnull
  private static List<String> asExpressions(@Nonnull final List<FhirPathWithTag> paths) {
    return paths.stream().map(FhirPathWithTag::toExpression).toList();
  }

  /**
   * Creates a tree node with the given prefix and children, optimizing for the case
   * where there is only a single child.
   * <p>
   * If there is only one child and it's not a null path, this method will combine the
   * prefix with the child's path rather than creating a separate node. This optimization
   * helps to flatten the tree where possible.
   *
   * @param prefix The prefix path to use for the node
   * @param children The child nodes to include
   * @return An optimized tree node
   * @throws IllegalStateException if the children list is empty
   */
  @Nonnull
  private static Tree<FhirPathWithTag> maybeUnnestingNode(
      @Nonnull final FhirPath prefix,
      @Nonnull final List<Tree<FhirPathWithTag>> children) {
    if (children.isEmpty()) {
      throw new IllegalStateException("Empty children list passed to maybeUnnestingNode");
    } else if (children.size() == 1 && !children.get(0).getValue().isNull()) {
      // Optimization: If there's only one child, combine the prefix with the child's path
      return children.get(0).mapValue(v -> v.withPrefix(prefix));
    } else {
      // Create a new node with the prefix and all children
      return Tree.node(FhirPathWithTag.of(prefix), children);
    }
  }

  /**
   * Core recursive method that performs the unnesting of paths into a tree structure.
   * <p>
   * This method:
   * 1. Separates paths into leaf paths (like $this) and unnestable paths
   * 2. Groups unnestable paths by their common prefixes to maintain data relationships
   * 3. Processes function paths separately from regular paths
   * 4. Recursively processes the suffixes of each group
   * 5. Combines the results into a unified tree structure
   * <p>
   * The grouping by common prefixes is crucial for ensuring that only valid combinations
   * of values appear in the extracted data, rather than a Cartesian product of all values.
   *
   * @param paths The list of paths to unnest
   * @return A list of tree nodes representing the unnested paths
   */
  @Nonnull
  List<Tree<FhirPathWithTag>> unnestPathsInternal(@Nonnull final List<FhirPathWithTag> paths) {
    log.trace("Unnesting paths: {}", asExpressions(paths));
    if (paths.isEmpty()) {
      return Collections.emptyList();
    } else {
      // Split the paths into two groups:
      // 1. Leaf paths ($this and empty paths) that should be returned as-is
      // 2. Unnestable paths that need to check for common prefixes with other paths

      // Extract leaf paths (null paths like $this)
      final List<FhirPathWithTag> leafPaths = paths.stream()
          .filter(FhirPathWithTag::isNull)
          .toList();

      // Convert leaf paths to leaf nodes
      final List<Leaf<FhirPathWithTag>> leafNodes = leafPaths.stream().map(Leaf::of).toList();

      // Extract paths that can be unnested (non-null paths)
      final List<FhirPathWithTag> unnestablePaths = paths.stream()
          .filter(Predicate.not(FhirPathWithTag::isNull))
          .toList();

      // Group unnestable paths by their common prefixes
      // This is the key step for identifying shared path segments
      final Map<FhirPath, List<FhirPathWithTag>> groupedPaths = unnestablePaths.stream()
          .collect(
              groupingBy(FhirPathWithTag::prefix, LinkedHashMap::new,
                  mapping(FhirPathWithTag::suffix, toList())));
      
      // Process each group of paths with a common prefix
      final List<Tree<FhirPathWithTag>> unnestedNodes = groupedPaths.entrySet().stream()
          .flatMap(entry -> {
                // 1. Identify suffixes that are functions and must not be unnested
                // Functions require special handling to maintain their semantics
                final List<FhirPathWithTag> funcSuffixes = entry.getValue().stream()
                    .filter(ImplicitUnnester::startsWithFunction)
                    .toList();
                
                // 2. Process function suffixes recursively and prepend the current prefix
                final List<Tree<FhirPathWithTag>> funcNodes = unnestPathsInternal(funcSuffixes)
                    .stream()
                    .map(tn -> tn.mapValue(v -> v.withPrefix(entry.getKey())))
                    .toList();
                
                // 3. Identify regular suffixes that need to be unnested
                final List<FhirPathWithTag> suffixesToUnnest = entry.getValue().stream()
                    .filter(Predicate.not(ImplicitUnnester::startsWithFunction))
                    .toList();
                
                // 4. Process regular suffixes recursively and create unnesting nodes
                final Stream<Tree<FhirPathWithTag>> unnestedNodesStream =
                    suffixesToUnnest.isEmpty()
                    ? Stream.empty()
                    : Stream.of(maybeUnnestingNode(entry.getKey(),
                        unnestPathsInternal(suffixesToUnnest)));
                
                final List<Tree<FhirPathWithTag>> unnestNodes = unnestedNodesStream.toList();
                
                // 5. Combine function nodes and regular unnested nodes
                return Stream.concat(unnestNodes.stream(), funcNodes.stream());
              }
          ).toList();
      
      // Combine leaf nodes and unnested nodes into the final result
      return Stream.concat(leafNodes.stream(), unnestedNodes.stream()).toList();
    }
  }

  /**
   * Determines if a path starts with a function call.
   * <p>
   * Functions require special handling during unnesting because they can change
   * the semantics of the path. For example, `first().name` should not be unnested
   * the same way as `name.given` because the function call changes the context.
   *
   * @param path The path to check
   * @return true if the path starts with a function call, false otherwise
   */
  private static boolean startsWithFunction(@Nonnull final FhirPathWithTag path) {
    return (path.getPath().head() instanceof Paths.EvalFunction);
  }
}
