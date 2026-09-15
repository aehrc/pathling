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

import au.csiro.pathling.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.definition.defaults.DefaultPrimitiveDefinition;
import au.csiro.pathling.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.definition.defaults.DefaultResourceTag;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;

/**
 * Tests that the reported type is independent of the R4 enumeration, and that the two
 * implementations of the abstraction describe the same resource in the same way.
 */
class DefinitionContextAgreementTest {

  /** A type code that the R4 enumeration does not contain. */
  @Nonnull private static final String UNKNOWN_TYPE_CODE = "MoneyQuantity2";

  @Nonnull
  private static final DefinitionContext FHIR_DEFINITIONS =
      FhirDefinitionContext.of(FhirContext.forR4());

  /**
   * The elements of Basic in R4, with the cardinality the specification gives each of them, where
   * -1 stands for an unbounded maximum.
   */
  @Nonnull
  private static final DefinitionContext EXPLICIT_DEFINITIONS =
      DefaultDefinitionContext.of(
          DefaultResourceDefinition.of(
              DefaultResourceTag.of("Basic"),
              DefaultPrimitiveDefinition.single("id", FhirType.ID),
              DefaultPrimitiveDefinition.single("meta", FhirType.of("Meta")),
              DefaultPrimitiveDefinition.single("implicitRules", FhirType.URI),
              DefaultPrimitiveDefinition.single("language", FhirType.CODE),
              DefaultPrimitiveDefinition.single("text", FhirType.of("Narrative")),
              DefaultPrimitiveDefinition.of("contained", FhirType.of("Resource"), -1),
              DefaultPrimitiveDefinition.of("extension", FhirType.EXTENSION, -1),
              DefaultPrimitiveDefinition.of("modifierExtension", FhirType.EXTENSION, -1),
              DefaultPrimitiveDefinition.of("identifier", FhirType.IDENTIFIER, -1),
              DefaultPrimitiveDefinition.single("code", FhirType.CODEABLECONCEPT),
              DefaultPrimitiveDefinition.single("subject", FhirType.REFERENCE),
              DefaultPrimitiveDefinition.single("created", FhirType.DATETIME),
              DefaultPrimitiveDefinition.single("author", FhirType.REFERENCE)));

  @Nonnull
  private static List<String> childNames(@Nonnull final NodeDefinition definition) {
    return definition.getChildren().stream().map(ChildDefinition::getName).toList();
  }

  @Nonnull
  private static List<Integer> cardinalities(@Nonnull final NodeDefinition definition) {
    return definition.getChildren().stream()
        .map(ElementDefinition.class::cast)
        .map(ElementDefinition::getMaxCardinality)
        .toList();
  }

  @Test
  void representsATypeCodeOutsideTheR4Enumeration() {
    // The premise of the test: the code is genuinely outside the enumeration.
    assertTrue(
        Stream.of(FHIRDefinedType.values())
            .noneMatch(definedType -> UNKNOWN_TYPE_CODE.equals(definedType.toCode())));

    final FhirType unknownType = FhirType.of(UNKNOWN_TYPE_CODE);
    assertEquals(UNKNOWN_TYPE_CODE, unknownType.toCode());

    final ElementDefinition element = DefaultPrimitiveDefinition.single("value", unknownType);
    assertEquals(unknownType, element.getFhirType().orElseThrow());
    assertFalse(FhirType.STRING.equals(unknownType));
  }

  @Test
  void bothImplementationsEnumerateTheSameChildren() {
    assertEquals(
        childNames(EXPLICIT_DEFINITIONS.findResourceDefinition("Basic")),
        childNames(FHIR_DEFINITIONS.findResourceDefinition("Basic")));
  }

  @Test
  void bothImplementationsReportTheSameCardinality() {
    assertEquals(
        cardinalities(EXPLICIT_DEFINITIONS.findResourceDefinition("Basic")),
        cardinalities(FHIR_DEFINITIONS.findResourceDefinition("Basic")));
  }
}
