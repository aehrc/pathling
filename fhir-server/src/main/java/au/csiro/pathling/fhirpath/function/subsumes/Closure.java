package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import com.google.common.collect.Streams;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a relation defined by a transitive closure table.
 *
 * @author Piotr Szul
 */
class Closure {

  private final Map<SimpleCoding, List<SimpleCoding>> mappings;

  private Closure(Map<SimpleCoding, List<SimpleCoding>> mappings) {
    this.mappings = mappings;
  }

  Map<SimpleCoding, List<SimpleCoding>> getMappings() {
    return mappings;
  }

  /**
   * Set of codings with contains() following the coding's equivalence semantics.
   *
   * @author Piotr Szul
   */
  static class CodingSet {

    private final Set<SimpleCoding> allCodings;
    private final Set<SimpleCoding> unversionedCodings;

    CodingSet(Set<SimpleCoding> allCodings) {
      this.allCodings = allCodings;
      this.unversionedCodings =
          allCodings.stream().map(SimpleCoding::toNonVersioned).collect(Collectors.toSet());
    }

    /**
     * Belongs to set operation with the coding's equivalence semantics, i.e. if the set includes an
     * unversioned coding, it contains any versioned coding with the same system code, and; if a set
     * contains a versioned coding, it contains its corresponding unversioned coding as well.
     *
     * @param c coding
     */
    boolean contains(SimpleCoding c) {
      return allCodings.contains(c) || (c.isVersioned()
                                        ? allCodings.contains(c.toNonVersioned())
                                        : unversionedCodings.contains(c));
    }
  }

  /**
   * Expands given set of Codings using with the closure, that is produces a set of Codings that are
   * in the relation with the given set.
   */
  public Set<SimpleCoding> expand(Set<SimpleCoding> codings) {
    final Closure.CodingSet baseSet = new Closure.CodingSet(codings);
    return Streams
        .concat(codings.stream(), mappings.entrySet().stream()
            .filter(kv -> baseSet.contains(kv.getKey())).flatMap(kv -> kv.getValue().stream()))
        .collect(Collectors.toSet());
  }

  /**
   * Checks if any of the Codings in the right set is in the relation with any of the Codings in the
   * left set.
   */
  public boolean anyRelates(Collection<SimpleCoding> left, Collection<SimpleCoding> right) {
    // filter out null SystemAndCodes
    Set<SimpleCoding> leftSet =
        left.stream().filter(SimpleCoding::isNotNull).collect(Collectors.toSet());
    final Closure.CodingSet expansion = new Closure.CodingSet(expand(leftSet));
    return right.stream().anyMatch(expansion::contains);
  }

  @Override
  public String toString() {
    return "Closure [mappings=" + mappings + "]";
  }

  public static Closure fromMappings(List<Mapping> mappings) {
    Map<SimpleCoding, List<Mapping>> groupedMappings =
        mappings.stream().collect(Collectors.groupingBy(Mapping::getFrom));
    Map<SimpleCoding, List<SimpleCoding>> groupedCodings =
        groupedMappings.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream()
                .map(Mapping::getTo)
                .collect(Collectors.toList())));
    return new Closure(groupedCodings);
  }
}
