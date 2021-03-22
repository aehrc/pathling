/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import com.google.common.collect.Streams;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.ToString;

/**
 * Represents a relation between codings. It my be a transitive or non-transitive relation depending
 * on the construction, i.e: the creator is responsible for explicitly defining all related pair for
 * transitive closure.
 *
 * @author Piotr Szul
 */
@ToString
public class Relation {

  @Nonnull
  private final Map<SimpleCoding, List<SimpleCoding>> mappings;

  private Relation(@Nonnull final Map<SimpleCoding, List<SimpleCoding>> mappings) {
    this.mappings = mappings;
  }

  @Nonnull
  Map<SimpleCoding, List<SimpleCoding>> getMappings() {
    return mappings;
  }

  /**
   * Set of codings with contains() following the coding's equivalence semantics.
   *
   * @author Piotr Szul
   */
  static class CodingSet {

    @Nonnull
    private final Set<SimpleCoding> allCodings;
    @Nonnull
    private final Set<SimpleCoding> unversionedCodings;

    CodingSet(@Nonnull final Set<SimpleCoding> allCodings) {
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
    boolean contains(@Nonnull final SimpleCoding c) {
      return allCodings.contains(c) || (c.isVersioned()
                                        ? allCodings.contains(c.toNonVersioned())
                                        : unversionedCodings.contains(c));
    }
  }

  @Nonnull
  public static Relation fromMappings(@Nonnull final Collection<Mapping> mappings) {
    final Map<SimpleCoding, List<Mapping>> groupedMappings =
        mappings.stream().collect(Collectors.groupingBy(Mapping::getFrom));
    final Map<SimpleCoding, List<SimpleCoding>> groupedCodings =
        groupedMappings.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stream()
                .map(Mapping::getTo)
                .collect(Collectors.toList())));
    return new Relation(groupedCodings);
  }

  @Nonnull
  public static Relation empty() {
    return new Relation(Collections.emptyMap());
  }

  /**
   * Checks if any of the Codings in the right set is in the relation with any of the Codings in the
   * left set.
   */
  public boolean anyRelates(@Nonnull final Collection<SimpleCoding> left,
      @Nonnull final Collection<SimpleCoding> right) {
    // filter out null SystemAndCodes
    final Set<SimpleCoding> leftSet =
        left.stream().filter(SimpleCoding::isDefined).collect(Collectors.toSet());
    final Relation.CodingSet expansion = new Relation.CodingSet(expand(leftSet));
    return right.stream().anyMatch(expansion::contains);
  }

  /**
   * Expands given set of Codings using with the closure, that is produces a set of Codings that are
   * in the relation with the given set.
   */
  @Nonnull
  private Set<SimpleCoding> expand(@Nonnull final Set<SimpleCoding> codings) {
    final Relation.CodingSet baseSet = new Relation.CodingSet(codings);
    return Streams
        .concat(codings.stream(), mappings.entrySet().stream()
            .filter(kv -> baseSet.contains(kv.getKey())).flatMap(kv -> kv.getValue().stream()))
        .collect(Collectors.toSet());
  }
}
