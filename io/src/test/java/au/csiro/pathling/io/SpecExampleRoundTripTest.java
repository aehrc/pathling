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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.transform.NonConformantContent;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Round-trips the FHIR R4 specification examples, which is the corpus US2 names alongside Synthea
 * (FR-016, SC-001).
 *
 * <p>The two corpora fail differently, which is why both exist. Synthea is one generator's idea of
 * FHIR: high volume, narrow variety, the same handful of profiles repeated. These examples are the
 * opposite — hand-authored by the specification's editors to demonstrate each element, and
 * therefore carrying the corners no generator emits. The subset vendored here covers every
 * non-{@code Bundle} resource type once and then every structural corner the selection names;
 * {@code MANIFEST.tsv} beside the data records what each file is there to prove.
 *
 * <p>The detection runs over every type the corpus carries, and the round trip over a structural
 * subset of them: the fewest types that between them cover every corner {@code MANIFEST.tsv} names.
 * The corpus is kept whole on disk so that restoring full coverage is a change to the selection
 * rather than a re-vendoring (decision 68).
 *
 * <p>The exclusions below apply, and each is asserted rather than left to be incidental. An
 * exclusion that never fires is indistinguishable from one that was never needed, and the
 * difference matters when a carve-out is removed. Beside them, the two exceptions every round trip
 * carries are asserted too: a type whose examples write a decimal with a trailing zero or an
 * exponent must have a number that matched only numerically, and the base64Binary values carrying
 * whitespace are recorded by type.
 *
 * <ul>
 *   <li>{@code Bundle} is excluded permanently, per FR-007. A bundle is never stored as a resource
 *       type, so it can never round-trip as one; its contents round-trip as the resources it is
 *       exploded into, which is T058, T068 and T069a's concern in M3. The corpus carries bundles
 *       anyway, so the exclusion has something to act on.
 *   <li>{@code contained} resources are excluded permanently, per FR-006, which requires that they
 *       are not represented and that their presence is detected. Unlike the other two this is not a
 *       carve-out that M5 removes — it is the layout's stated behaviour, and what is asserted here
 *       is that the detection fires exactly where the corpus carries one.
 *   <li>Primitive id and extension content is excluded until M5, under FR-017's carve-out. Unlike
 *       the Synthea corpus these examples do carry it, so the exclusion count is positive for the
 *       types that carry it and zero for the rest. T078c removes this exclusion.
 * </ul>
 *
 * <p>{@code Parameters} is excluded from the round trip alone, its detection still asserted, until
 * decision 64 is settled.
 *
 * <p>The findings are asserted as an exact set rather than as merely permissible content. A
 * permissive assertion would pass if the corpus stopped carrying contained resources altogether,
 * which is the vacuity these assertions exist to prevent.
 */
class SpecExampleRoundTripTest {

  /** The vendored corpus, one file of newline-delimited JSON per resource type. */
  @Nonnull private static final String CORPUS = "/data/fhir-spec/R4";

  /** The resource type a bundle is never stored as, per FR-007. */
  @Nonnull private static final String EXCLUDED_TYPE = "Bundle";

  /**
   * The type whose examples cannot yet round-trip, because an element holding an inline resource is
   * dropped for want of a type rather than named as one. Decision 64 and T134e.
   */
  @Nonnull private static final String UNREPRESENTABLE_TYPE = "Parameters";

  /**
   * Every piece of content in the corpus that this layout does not store, by the type carrying it.
   * A type absent from this map must produce no findings at all.
   *
   * <p>Three kinds appear. {@code contained} is FR-006 working as specified. The {@code _}-prefixed
   * paths are FR-017's carve-out, which T078c removes. {@code Parameters.parameter.resource} is
   * neither: the element is declared by the definitions but typed as the abstract {@code Resource},
   * which the definition abstraction drops for want of a type and the check then reports as
   * undescribed. Decision 64 records that mis-diagnosis and T134e raises it.
   */
  @Nonnull
  private static final Map<String, Set<String>> EXPECTED_FINDINGS =
      Map.ofEntries(
          Map.entry("ActivityDefinition", Set.of("ActivityDefinition.timingTiming._event")),
          Map.entry("CareTeam", Set.of("CareTeam.contained")),
          Map.entry("CoverageEligibilityResponse", Set.of("CoverageEligibilityResponse.contained")),
          Map.entry("DocumentManifest", Set.of("DocumentManifest.contained")),
          Map.entry("DocumentReference", Set.of("DocumentReference.contained")),
          Map.entry("GuidanceResponse", Set.of("GuidanceResponse.contained")),
          Map.entry("HealthcareService", Set.of("HealthcareService.contained")),
          Map.entry("MedicationAdministration", Set.of("MedicationAdministration.contained")),
          Map.entry("MedicationDispense", Set.of("MedicationDispense.contained")),
          Map.entry("MedicationKnowledge", Set.of("MedicationKnowledge.contained")),
          Map.entry("Parameters", Set.of("Parameters.parameter.resource")),
          Map.entry(
              "Patient",
              Set.of(
                  "Patient.contained",
                  "Patient._active",
                  "Patient.contact.name._family",
                  "Patient.contact.name._given")),
          Map.entry("PlanDefinition", Set.of("PlanDefinition.contained")),
          Map.entry("QuestionnaireResponse", Set.of("QuestionnaireResponse.contained")),
          Map.entry("RequestGroup", Set.of("RequestGroup.contained")),
          Map.entry("RiskAssessment", Set.of("RiskAssessment.contained")),
          Map.entry(
              "StructureDefinition",
              Set.of("StructureDefinition.differential.element.type._profile")));

  /**
   * The base64Binary values carrying whitespace, by the type carrying them. A type absent from this
   * map must carry none.
   */
  @Nonnull private static final Map<String, Integer> EXPECTED_BASE64_WHITESPACE = Map.of();

  @ParameterizedTest
  @MethodSource("resourceTypes")
  void detectsExactlyTheContentThisLayoutDoesNotStore(@Nonnull final String resourceType) {
    final List<NonConformantContent> findings =
        RoundTripHarness.excludingPrimitiveMetadata().findings(resourceType, corpus(resourceType));

    assertEquals(
        EXPECTED_FINDINGS.getOrDefault(resourceType, Set.of()),
        findings.stream().map(NonConformantContent::getPath).collect(Collectors.toSet()),
        "the content this layout does not store is not what was recorded for this type");
    assertTrue(
        findings.stream()
            .allMatch(
                finding ->
                    finding.isContainedResource()
                        || finding.isPrimitiveMetadata()
                        || finding.isUndescribedContent()),
        "a finding of an unrecorded kind: " + findings);
  }

  @ParameterizedTest
  @MethodSource("selection")
  void roundTripsTheStructuralSubset(@Nonnull final String resourceType) {
    final Path corpus = corpus(resourceType);
    final RoundTripHarness harness =
        RoundTripHarness.excludingPrimitiveMetadata().excludingContainedResources();

    final RoundTripOutcome outcome = harness.assertRoundTrip(resourceType, corpus);
    assertEquals(
        EXPECTED_BASE64_WHITESPACE.getOrDefault(resourceType, 0),
        outcome.getBase64Whitespace(),
        "the base64Binary values carrying whitespace are not what was recorded for this type");
    if (carriesDecimalLexicalForm(resourceType)) {
      assertTrue(
          outcome.getNumericOnly() > 0,
          "the examples of this type write a decimal in a form a double does not reproduce, so"
              + " some number must have matched numerically and not textually");
    }
    final int excluded = outcome.getPrimitiveMetadata();
    if (carriesPrimitiveMetadata(resourceType)) {
      assertTrue(
          excluded > 0,
          "the examples of this type carry primitive id or extension content, so the carve-out"
              + " must have applied");
    } else {
      assertEquals(
          0, excluded, "the examples of this type carry no primitive id or extension content");
    }
  }

  /**
   * Asserts the bundle exclusion is real rather than incidental: the corpus carries bundles, and
   * they are deliberately not among the types round-tripped.
   */
  @Test
  void excludesBundlePermanently() {
    assertFalse(
        lines(corpus(EXCLUDED_TYPE)).isEmpty(),
        "the corpus must carry bundles, or the exclusion has nothing to act on");
    assertFalse(
        resourceTypes().anyMatch(EXCLUDED_TYPE::equals),
        "a bundle is never stored as a resource type, so it cannot be round-tripped as one");
  }

  /**
   * Asserts the inline-resource exclusion is real: the corpus carries the type, the element is
   * detected as content this layout does not store, and the type is deliberately not round-tripped.
   */
  @Test
  void excludesTheTypeHoldingAnInlineResource() {
    assertFalse(
        lines(corpus(UNREPRESENTABLE_TYPE)).isEmpty(),
        "the corpus must carry this type, or the exclusion has nothing to act on");
    assertEquals(
        Set.of("Parameters.parameter.resource"),
        EXPECTED_FINDINGS.get(UNREPRESENTABLE_TYPE),
        "the exclusion covers the comparison, so the detection must still be asserted");
    assertFalse(
        roundTrippableTypes().anyMatch(UNREPRESENTABLE_TYPE::equals),
        "an element dropped for want of a type cannot round-trip until decision 64 is settled");
  }

  @Test
  void selectsEveryStructuralCornerTheCorpusCovers() {
    final Set<String> all =
        roundTrippableTypes()
            .flatMap(type -> corners().getOrDefault(type, Set.of()).stream())
            .collect(Collectors.toSet());
    final Set<String> selected =
        selection()
            .flatMap(type -> corners().getOrDefault(type, Set.of()).stream())
            .collect(Collectors.toSet());

    assertEquals(all, selected, "the subset must cover every corner the full corpus covers");
    assertTrue(selection().count() < roundTrippableTypes().count(), "the subset is a subset");
  }

  /** The types the round trip could run over, which is every type the corpus carries less two. */
  @Nonnull
  private static Stream<String> roundTrippableTypes() {
    return resourceTypes().filter(type -> !UNREPRESENTABLE_TYPE.equals(type));
  }

  /**
   * The types the round trip runs over: a greedy cover of every structural corner {@code
   * MANIFEST.tsv} names, taken over the round-trippable types (decision 68, T076).
   *
   * <p>The corpus is kept whole on disk and the subset chosen here, so restoring full coverage is a
   * matter of returning {@link #roundTrippableTypes()} instead. Each step takes the type covering
   * the most corners not yet covered, breaking ties by name, so the selection is deterministic.
   */
  @Nonnull
  private static Stream<String> selection() {
    final Map<String, Set<String>> corners = corners();
    final List<String> candidates = roundTrippableTypes().toList();
    final Set<String> uncovered =
        candidates.stream()
            .flatMap(type -> corners.getOrDefault(type, Set.of()).stream())
            .collect(Collectors.toCollection(HashSet::new));
    final List<String> selected = new ArrayList<>();
    while (!uncovered.isEmpty()) {
      final String next =
          candidates.stream()
              .max(
                  Comparator.<String>comparingLong(
                          type ->
                              corners.getOrDefault(type, Set.of()).stream()
                                  .filter(uncovered::contains)
                                  .count())
                      .thenComparing(Comparator.<String>reverseOrder()))
              .orElseThrow();
      selected.add(next);
      uncovered.removeAll(corners.getOrDefault(next, Set.of()));
    }
    return selected.stream().sorted();
  }

  /**
   * The structural corners each resource type's examples cover, as {@code MANIFEST.tsv} records.
   */
  @Nonnull
  private static Map<String, Set<String>> corners() {
    final List<String> rows = lines(directory().resolve("MANIFEST.tsv"));
    return rows.subList(1, rows.size()).stream()
        .map(row -> row.split("\t"))
        .collect(
            Collectors.groupingBy(
                columns -> columns[1],
                Collectors.flatMapping(
                    columns -> Stream.of(columns[5].split(";")), Collectors.toSet())));
  }

  /**
   * Whether the examples of a type write a decimal with a trailing zero or in exponent notation,
   * neither of which a double reproduces.
   */
  private static boolean carriesDecimalLexicalForm(@Nonnull final String resourceType) {
    final Set<String> covered = corners().getOrDefault(resourceType, Set.of());
    return covered.contains("decimal.trailing-zero") || covered.contains("decimal.exponent");
  }

  private static boolean carriesPrimitiveMetadata(@Nonnull final String resourceType) {
    return EXPECTED_FINDINGS.getOrDefault(resourceType, Set.of()).stream()
        .anyMatch(path -> path.contains("._"));
  }

  /** Every resource type the corpus carries, less the one that is permanently excluded. */
  @Nonnull
  private static Stream<String> resourceTypes() {
    try (var entries = Files.list(directory())) {
      return entries
          .map(entry -> entry.getFileName().toString())
          .filter(name -> name.endsWith(".ndjson"))
          .map(name -> name.substring(0, name.length() - ".ndjson".length()))
          .filter(name -> !EXCLUDED_TYPE.equals(name))
          .sorted()
          .toList()
          .stream();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static List<String> lines(@Nonnull final Path corpus) {
    try {
      return Files.readAllLines(corpus).stream().filter(line -> !line.isBlank()).toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static Path corpus(@Nonnull final String resourceType) {
    return directory().resolve(resourceType + ".ndjson");
  }

  @Nonnull
  private static Path directory() {
    try {
      return Path.of(
          Objects.requireNonNull(
                  SpecExampleRoundTripTest.class.getResource(CORPUS), "The corpus is not readable")
              .toURI());
    } catch (final URISyntaxException e) {
      throw new IllegalStateException("The corpus is not readable: " + CORPUS, e);
    }
  }
}
