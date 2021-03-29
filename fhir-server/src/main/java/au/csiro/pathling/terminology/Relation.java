/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import com.google.common.collect.Streams;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.*;

/**
 * Represents relation between codings with implicit coding equality.
 * <p>
 * Implicitly two codings that are equal are related. It may be a transitive or non-transitive
 * relation depending on the construction, i.e: the creator is responsible for explicitly defining
 * all related pairs for transitive closure (except of the equality).
 *
 * @author Piotr Szul
 */
@ToString
@EqualsAndHashCode
public class Relation {

  /**
   * An entry representing the existence of relation between <code>form</code> and <code>to</code>.
   */
  @Data
  @AllArgsConstructor(staticName = "of")
  public static class Entry implements Serializable {

    private static final long serialVersionUID = 1L;

    @NonNull
    private final SimpleCoding from;

    @NonNull
    private final SimpleCoding to;
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
  private final Map<SimpleCoding, List<SimpleCoding>> mappings;

  /**
   * Private constructor. Use {@link #equality()} or {@link #fromMappings} to create instances.
   */
  private Relation(@Nonnull final Map<SimpleCoding, List<SimpleCoding>> mappings) {
    this.mappings = mappings;
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


  /**
   * Checks if any of the Codings in the right set is in the relation with any of the Codings in the
   * left set.
   *
   * @param left a collections of codings.
   * @param right a collection of codings.
   * @return true if any left coding is related to any right coding.
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
   * Constructs relation from given list of related pairs of codings.
   * <p>
   * The relation is assumed to include coding equality so <code>(A,A)</code> is assumed. All other
   * the related pairs need to be explicitly listed. If the relation is meant to represent a
   * transitive closure with implicit equality such that:
   * <code> A -> B -> C </code> than the entry list must to include the all pairs of:
   * <code>[(A,B) , (B,C), (A, C)]</code>.
   *
   * @param entries the list of pair of codings that are related.
   * @return the relation instance.
   */
  @Nonnull
  public static Relation fromMappings(@Nonnull final Collection<Entry> entries) {
    final Map<SimpleCoding, List<Entry>> groupedMappings =
        entries.stream().collect(Collectors.groupingBy(Entry::getFrom));
    final Map<SimpleCoding, List<SimpleCoding>> groupedCodings =
        groupedMappings.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream()
                .map(Entry::getTo)
                .collect(Collectors.toList())));
    return new Relation(groupedCodings);
  }

  /**
   * Constructs a relation that only includes coding equality.
   *
   * @return the equality relation instance.
   */
  @Nonnull
  public static Relation equality() {
    return new Relation(Collections.emptyMap());
  }
}
