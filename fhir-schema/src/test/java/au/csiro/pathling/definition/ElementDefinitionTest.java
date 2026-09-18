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
package au.csiro.pathling.definition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.defaults.DefaultPrimitiveDefinition;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;

/**
 * Tests the cardinality reported by an element definition, which determines whether an element is
 * stored as a scalar or as an array.
 */
class ElementDefinitionTest {

  @Nonnull
  private static final DefinitionContext DEFINITIONS =
      FhirDefinitionContext.of(FhirContext.forR4());

  @Nonnull
  private static ElementDefinition elementOf(
      @Nonnull final NodeDefinition parent, @Nonnull final String name) {
    return (ElementDefinition) parent.getChildElement(name).orElseThrow();
  }

  @Test
  void reportsASingularElementAsNotRepeating() {
    final ElementDefinition birthDate =
        elementOf(DEFINITIONS.findResourceDefinition("Patient"), "birthDate");
    assertEquals(1, birthDate.getMaxCardinality());
    assertFalse(birthDate.isRepeating());
  }

  @Test
  void reportsARepeatingElementAsUnbounded() {
    final ElementDefinition name = elementOf(DEFINITIONS.findResourceDefinition("Patient"), "name");
    assertEquals(-1, name.getMaxCardinality());
    assertTrue(name.isRepeating());
  }

  @Test
  void reportsTheCardinalityOfARepeatingBackboneElement() {
    final ElementDefinition contact =
        elementOf(DEFINITIONS.findResourceDefinition("Patient"), "contact");
    assertTrue(contact.isRepeating());

    // Cardinality within the backbone element is independent of the cardinality of the element
    // itself.
    assertFalse(elementOf(contact, "gender").isRepeating());
    assertTrue(elementOf(contact, "telecom").isRepeating());
  }

  @Test
  void reportsTheCardinalityOfAChoiceThroughItsVariants() {
    final ChoiceDefinition deceased =
        (ChoiceDefinition)
            DEFINITIONS.findResourceDefinition("Patient").getChildElement("deceased").orElseThrow();

    // Every variant of a choice carries the cardinality of the choice itself.
    deceased
        .getAllChildTypes()
        .forEach(
            variant -> {
              assertEquals(1, variant.getMaxCardinality());
              assertFalse(variant.isRepeating());
            });
    assertFalse(deceased.getChildByType("boolean").orElseThrow().isRepeating());
  }

  @Test
  void reportsTheCardinalityOfAnExplicitlyDefinedElement() {
    assertFalse(DefaultPrimitiveDefinition.single("family", FHIRDefinedType.STRING).isRepeating());
    assertEquals(
        1, DefaultPrimitiveDefinition.single("family", FHIRDefinedType.STRING).getMaxCardinality());
    assertTrue(DefaultPrimitiveDefinition.of("given", FHIRDefinedType.STRING, -1).isRepeating());
    assertEquals(
        -1, DefaultPrimitiveDefinition.of("given", FHIRDefinedType.STRING, -1).getMaxCardinality());
  }
}
