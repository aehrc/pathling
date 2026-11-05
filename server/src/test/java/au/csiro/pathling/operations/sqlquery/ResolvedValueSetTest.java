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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ResolvedValueSet#describeContent()}, which keys export jobs on the
 * membership a value set actually resolved to, so it must be stable across member order and
 * sensitive to every column of every member.
 *
 * @author John Grimes
 */
class ResolvedValueSetTest {

  private static final String URL = "http://example.org/ValueSet/cvd";

  private static final String KEY = URL + "|2026";

  private static final String SNOMED = "http://snomed.info/sct";

  private static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";

  private static final ValueSetMember MI =
      new ValueSetMember(SNOMED, "20260131", "22298006", "Myocardial infarction", null);

  private static final ValueSetMember I21 =
      new ValueSetMember(ICD10, null, "I21", "Acute myocardial infarction", null);

  @Test
  void describeContentStartsWithTheValueSetPrefix() {
    // The prefix separates a value set from the other leaf kinds in a graph description, so two
    // nodes of different kinds cannot collide on a hash.
    assertThat(resolved(MI, I21).describeContent()).startsWith("value-set:");
  }

  @Test
  void describeContentIsIdenticalForTheSameMembersInADifferentOrder() {
    // Two sources may list the same membership in a different order; the membership is the same,
    // so an export job for one must be reusable for the other.
    assertThat(resolved(MI, I21).describeContent()).isEqualTo(resolved(I21, MI).describeContent());
  }

  @Test
  void describeContentIsIdenticalForIdenticalMemberships() {
    assertThat(resolved(MI, I21).describeContent()).isEqualTo(resolved(MI, I21).describeContent());
  }

  @Test
  void describeContentDiffersWhenACodeDiffers() {
    final ValueSetMember other =
        new ValueSetMember(SNOMED, "20260131", "73211009", "Myocardial infarction", null);

    assertThat(resolved(MI, I21).describeContent())
        .isNotEqualTo(resolved(other, I21).describeContent());
  }

  @Test
  void describeContentDiffersWhenAVersionDiffers() {
    final ValueSetMember other =
        new ValueSetMember(SNOMED, "20250131", "22298006", "Myocardial infarction", null);

    assertThat(resolved(MI, I21).describeContent())
        .isNotEqualTo(resolved(other, I21).describeContent());
  }

  @Test
  void describeContentDiffersBetweenANullAndANonNullVersion() {
    final ValueSetMember other =
        new ValueSetMember(SNOMED, null, "22298006", "Myocardial infarction", null);

    assertThat(resolved(MI, I21).describeContent())
        .isNotEqualTo(resolved(other, I21).describeContent());
  }

  @Test
  void describeContentDiffersWhenADisplayDiffers() {
    // Display is informative for membership but it is part of the rows the relation produces, so a
    // job that ran with one display cannot serve a request that inlined another.
    final ValueSetMember other =
        new ValueSetMember(SNOMED, "20260131", "22298006", "Heart attack", null);

    assertThat(resolved(MI, I21).describeContent())
        .isNotEqualTo(resolved(other, I21).describeContent());
  }

  @Test
  void describeContentDiffersWhenInactiveDiffers() {
    final ValueSetMember inactive =
        new ValueSetMember(SNOMED, "20260131", "22298006", "Myocardial infarction", true);
    final ValueSetMember active =
        new ValueSetMember(SNOMED, "20260131", "22298006", "Myocardial infarction", false);

    assertThat(resolved(MI, I21).describeContent())
        .isNotEqualTo(resolved(inactive, I21).describeContent())
        .isNotEqualTo(resolved(active, I21).describeContent());
    assertThat(resolved(inactive, I21).describeContent())
        .isNotEqualTo(resolved(active, I21).describeContent());
  }

  @Test
  void describeContentDiffersWhenAMemberIsMissing() {
    assertThat(resolved(MI, I21).describeContent()).isNotEqualTo(resolved(MI).describeContent());
  }

  @Test
  void describeContentDoesNotDependOnProvenance() {
    // Provenance is logged, not keyed on: the same membership from two expansions with different
    // identifiers and timestamps produces the same rows.
    final ValueSetExpansion first =
        new ValueSetExpansion(
            URL, "2026", "urn:uuid:1", "2026-01-01T00:00:00Z", List.of(), members(MI));
    final ValueSetExpansion second =
        new ValueSetExpansion(
            URL, "2026", "urn:uuid:2", "2026-02-02T00:00:00Z", List.of(), members(MI));

    assertThat(new ResolvedValueSet(KEY, first).describeContent())
        .isEqualTo(new ResolvedValueSet(KEY, second).describeContent());
  }

  @Nonnull
  private static ResolvedValueSet resolved(@Nonnull final ValueSetMember... members) {
    return new ResolvedValueSet(
        KEY, new ValueSetExpansion(URL, "2026", null, null, List.of(), members(members)));
  }

  @Nonnull
  private static List<ValueSetMember> members(@Nonnull final ValueSetMember... members) {
    return List.of(members);
  }
}
