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

package au.csiro.pathling.terminology.expand;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;

/**
 * Accumulates the members of an expansion from one or more sets of {@code contains} entries. It
 * flattens nested entries at every depth, contributes no member for an abstract entry, keeps the
 * first occurrence of each identity, and enforces the caller's member limit. One accumulator serves
 * a supplied expansion and the pages of a server expansion alike, so the rules exist once.
 *
 * @author John Grimes
 */
final class ExpansionAccumulator {

  private final int maxMembers;
  @Nonnull private final Set<String> seen = new HashSet<>();
  @Nonnull private final List<ValueSetMember> members = new ArrayList<>();

  /**
   * Creates an accumulator that accepts at most the given number of distinct members.
   *
   * @param maxMembers the largest membership the caller will accept
   */
  ExpansionAccumulator(final int maxMembers) {
    this.maxMembers = maxMembers;
  }

  /**
   * Adds the members described by a set of {@code contains} entries and everything nested within
   * them.
   *
   * @param contains the entries
   * @return the number of entries visited at every depth, abstract entries included
   * @throws ValueSetExpansionException if a non-abstract entry lacks a code, or an entry carries a
   *     code without a system
   */
  int addContains(@Nonnull final List<ValueSetExpansionContainsComponent> contains) {
    int visited = 0;
    for (final ValueSetExpansionContainsComponent entry : contains) {
      visited++;
      if (!entry.getAbstract()) {
        add(toMember(entry));
      }
      visited += addContains(entry.getContains());
    }
    return visited;
  }

  /**
   * Adds a member unless one with the same identity has already been added.
   *
   * @param member the member
   */
  void add(@Nonnull final ValueSetMember member) {
    if (seen.add(member.identity())) {
      members.add(member);
    }
  }

  /**
   * Checks the accumulated membership against the limit.
   *
   * @throws ExpansionLimitExceededException if more than {@code maxMembers} distinct members have
   *     been accumulated
   */
  void checkLimit() {
    if (members.size() > maxMembers) {
      throw new ExpansionLimitExceededException(maxMembers);
    }
  }

  /**
   * Returns the accumulated members in first-seen order.
   *
   * @return the members, as an unmodifiable list
   */
  @Nonnull
  List<ValueSetMember> members() {
    return Collections.unmodifiableList(members);
  }

  @Nonnull
  private static ValueSetMember toMember(@Nonnull final ValueSetExpansionContainsComponent entry) {
    if (!entry.hasCode()) {
      throw new ValueSetExpansionException("an entry does not identify a member: no code");
    }
    if (!entry.hasSystem()) {
      throw new ValueSetExpansionException(
          "an entry does not identify a member: no system for code '" + entry.getCode() + "'");
    }
    return new ValueSetMember(
        entry.getSystem(),
        entry.hasVersion() ? entry.getVersion() : null,
        entry.getCode(),
        entry.hasDisplay() ? entry.getDisplay() : null,
        entry.hasInactive() ? entry.getInactive() : null);
  }
}
