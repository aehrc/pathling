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

package au.csiro.pathling.schema.annotation;

import static au.csiro.pathling.schema.LayoutFields.CANONICAL_CODE_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.CANONICAL_VALUE_EXACT_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.END_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.NUMERIC_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.START_ANNOTATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.annotation.kinds.DateRangeKind;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Tests that the registry maps the annotation entries of the canonical structure to their kinds.
 */
class AnnotationRegistryTest {

  @Nonnull
  private static final DefinitionCanonicalStructure OBSERVATION =
      DefinitionCanonicalStructure.forResource(
          FhirDefinitionContext.of(FhirContext.forR4()), "Observation");

  @Test
  void eachBuiltInAnnotationIsComputedByItsKind() {
    final AnnotationRegistry registry = AnnotationRegistry.builtIn();
    final LayoutEntry quantity = OBSERVATION.entry("valueQuantity").orElseThrow();
    final DefinitionCanonicalStructure quantityStructure =
        OBSERVATION.elementStructure(quantity).orElseThrow();

    assertEquals(
        Map.of(START_ANNOTATION, DateRangeKind.INSTANCE, END_ANNOTATION, DateRangeKind.INSTANCE),
        kindsOf(registry, OBSERVATION.metadataGroup("_issued").orElseThrow().annotationEntries()));
    assertEquals(
        Map.of(NUMERIC_ANNOTATION, DecimalNumericKind.INSTANCE),
        kindsOf(
            registry, quantityStructure.metadataGroup("_value").orElseThrow().annotationEntries()));
    assertEquals(
        Map.of(
            CANONICAL_CODE_ANNOTATION,
            QuantityCanonicalKind.INSTANCE,
            CANONICAL_VALUE_EXACT_ANNOTATION,
            QuantityCanonicalKind.INSTANCE),
        kindsOf(
            registry,
            quantityStructure.entries().stream().filter(LayoutEntry::isAnnotation).toList()));
  }

  @Test
  void anEntryThatIsNotAnAnnotationHasNoKind() {
    final AnnotationRegistry registry = AnnotationRegistry.builtIn();
    assertEquals(Optional.empty(), registry.kindFor(OBSERVATION.entry("status").orElseThrow()));
    assertEquals(Optional.empty(), registry.kindFor(LayoutEntry.resourceType()));
  }

  @Test
  void anAnnotationNoKindIsGivenForHasNoKind() {
    final AnnotationRegistry registry = AnnotationRegistry.of(List.of(DecimalNumericKind.INSTANCE));
    final List<LayoutEntry> range =
        OBSERVATION.metadataGroup("_issued").orElseThrow().annotationEntries();
    assertEquals(Map.of(), kindsOf(registry, range));
  }

  @Test
  void twoKindsCarriedInTheSameFieldAreRejected() {
    final List<AnnotationKind> kinds = List.of(DateRangeKind.INSTANCE, DateRangeKind.INSTANCE);
    assertThrows(IllegalStateException.class, () -> AnnotationRegistry.of(kinds));
  }

  /** Maps the name of each entry a kind computes to that kind. */
  @Nonnull
  private static Map<String, AnnotationKind> kindsOf(
      @Nonnull final AnnotationRegistry registry, @Nonnull final List<LayoutEntry> entries) {
    return entries.stream()
        .filter(entry -> registry.kindFor(entry).isPresent())
        .collect(
            Collectors.toMap(LayoutEntry::getName, entry -> registry.kindFor(entry).orElseThrow()));
  }
}
