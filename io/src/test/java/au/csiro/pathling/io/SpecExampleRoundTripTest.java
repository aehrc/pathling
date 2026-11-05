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
 * <p>The round trip runs over every type the corpus carries, with FR-016's exceptions stripped from
 * the expected side and nothing else. Each exception is asserted rather than left to be incidental,
 * because an exclusion that never fires is indistinguishable from one that was never needed, and
 * the difference matters when an exception is removed. Beside them, the two exceptions every round
 * trip carries are asserted too: a type whose examples write a decimal with a trailing zero or an
 * exponent must have a number that matched only numerically, and the base64Binary values carrying
 * whitespace are recorded by type.
 *
 * <ul>
 *   <li>{@code Bundle} is excluded permanently, per FR-007. A bundle is never stored as a resource
 *       type, so it can never round-trip as one; its contents round-trip as the resources it is
 *       exploded into, which {@code IngestFormatRoundTripTest} covers. The corpus carries bundles
 *       anyway, so the exclusion has something to act on.
 *   <li>{@code contained} resources are excluded permanently, per FR-006. This is the layout's
 *       stated behaviour rather than a carve-out a later ticket removes.
 *   <li>An inline resource held by an element declared as the abstract {@code Resource}, which is
 *       {@code Parameters.parameter.resource} in this corpus, is excluded permanently.
 *   <li>Primitive id and extension content is excluded until ticket 17. Unlike the Synthea corpus
 *       these examples do carry it, so the exclusion count is positive for the types that carry it
 *       and zero for the rest.
 * </ul>
 */
class SpecExampleRoundTripTest {

  /** The vendored corpus, one file of newline-delimited JSON per resource type. */
  @Nonnull private static final String CORPUS = "/data/fhir-spec/R4";

  /** The resource type a bundle is never stored as, per FR-007. */
  @Nonnull private static final String EXCLUDED_TYPE = "Bundle";

  /** The types whose examples carry primitive id or extension content. */
  @Nonnull
  private static final Set<String> CARRYING_PRIMITIVE_METADATA =
      Set.of("ActivityDefinition", "Patient", "StructureDefinition");

  /** The types whose examples carry an inline resource beneath an abstract {@code Resource}. */
  @Nonnull private static final Set<String> CARRYING_INLINE_RESOURCES = Set.of("Parameters");

  /**
   * The types whose examples write a decimal with a trailing zero only where a double writes the
   * same text, as {@code TestReport.score} does with {@code 100.0}.
   */
  @Nonnull private static final Set<String> REPRODUCED_BY_A_DOUBLE = Set.of("TestReport");

  /**
   * The base64Binary values carrying whitespace, by the type carrying them. A type absent from this
   * map must carry none.
   */
  @Nonnull private static final Map<String, Integer> EXPECTED_BASE64_WHITESPACE = Map.of();

  @ParameterizedTest
  @MethodSource("resourceTypes")
  void roundTripsEveryResourceType(@Nonnull final String resourceType) {
    final Path corpus = corpus(resourceType);
    final RoundTripHarness harness =
        RoundTripHarness.excludingPrimitiveMetadata()
            .excludingContainedResources()
            .excludingInlineResources();

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
    assertEquals(
        CARRYING_PRIMITIVE_METADATA.contains(resourceType),
        outcome.getPrimitiveMetadata() > 0,
        "the primitive id and extension content excluded is not what was recorded for this type");
    assertEquals(
        CARRYING_INLINE_RESOURCES.contains(resourceType),
        outcome.getInlineResources() > 0,
        "the inline resources excluded are not what was recorded for this type");
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

  @Test
  void selectsEveryStructuralCornerTheCorpusCovers() {
    final Set<String> all =
        resourceTypes()
            .flatMap(type -> corners().getOrDefault(type, Set.of()).stream())
            .collect(Collectors.toSet());
    final Set<String> selected =
        selection()
            .flatMap(type -> corners().getOrDefault(type, Set.of()).stream())
            .collect(Collectors.toSet());

    assertEquals(all, selected, "the subset must cover every corner the full corpus covers");
    assertTrue(selection().count() < resourceTypes().count(), "the subset is a subset");
  }

  /**
   * The types the round trip through XML runs over: a greedy cover of every structural corner
   * {@code MANIFEST.tsv} names, taken over every type the corpus carries (decision 68, T076).
   *
   * <p>Each step takes the type covering the most corners not yet covered, breaking ties by name,
   * so the selection is deterministic. {@code IngestFormatRoundTripTest} runs it through XML.
   */
  @Nonnull
  static Stream<String> selection() {
    final Map<String, Set<String>> corners = corners();
    final List<String> candidates = resourceTypes().toList();
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
   * neither of which a double reproduces in general. A type whose only such decimal is one a double
   * does reproduce is left out.
   */
  private static boolean carriesDecimalLexicalForm(@Nonnull final String resourceType) {
    final Set<String> covered = corners().getOrDefault(resourceType, Set.of());
    return !REPRODUCED_BY_A_DOUBLE.contains(resourceType)
        && (covered.contains("decimal.trailing-zero") || covered.contains("decimal.exponent"));
  }

  /** Every resource type the corpus carries, less the one that is permanently excluded. */
  @Nonnull
  static Stream<String> resourceTypes() {
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
  static Path corpus(@Nonnull final String resourceType) {
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
